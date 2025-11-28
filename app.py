"""
Startup Command:
python3 -m aiohttp.web -H 0.0.0.0 -P 8000 app:init_func

"""

from asyncio.log import logger
import os
import json
import logging
from typing import Dict, List, Optional
from dotenv import load_dotenv
load_dotenv()
from aiohttp import web
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import GenieAPI
import asyncio
import sys
import traceback
from datetime import datetime, timezone, timedelta
from http import HTTPStatus
from aiohttp.web import Request, Response, json_response
from botbuilder.core import (
    BotFrameworkAdapterSettings,
    BotFrameworkAdapter,
    ActivityHandler,
    TurnContext,
)
from botbuilder.core.integration import aiohttp_error_middleware
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)
from botbuilder.schema import (
    Activity,
    ConversationReference,
    ActivityTypes,
    ChannelAccount,
    InvokeResponse,
)
import requests
import re

from config import DefaultConfig


CONFIG = DefaultConfig()


class UserSession:
    """Represents a user session with email-based identification"""
    def __init__(self, user_id: str, name: str = None):
        self.user_id = user_id  # Teams user ID
        #self.email = email
        self.name = name or "Usuario"
        self.conversation_id = None
        self.created_at = datetime.now(timezone.utc)
        self.last_activity = datetime.now(timezone.utc)
        self.is_authenticated = True  # Always true for Teams users
        self.user_context = {}
    
    def update_activity(self):
        """Update the last activity timestamp"""
        self.last_activity = datetime.now(timezone.utc)
    
    def to_dict(self):
        """Convert session to dictionary for logging/debugging"""
        return {
            "user_id": self.user_id,
            "name": self.name,
            "conversation_id": self.conversation_id,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "is_authenticated": self.is_authenticated
        }
    
    def get_display_name(self):
        """Get a friendly display name for the user"""
        return f"{self.name} ({self.uder_id})"

# For local development with Bot Framework Emulator, use BotFrameworkAdapter
if CONFIG.APP_ID and CONFIG.APP_PASSWORD:
    # Production: Use CloudAdapter
    ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))
else:
    # Local testing: Use BotFrameworkAdapter with empty credentials
    SETTINGS = BotFrameworkAdapterSettings("", "")
    ADAPTER = BotFrameworkAdapter(SETTINGS)


async def on_error(context: TurnContext, error: Exception):
    # This check writes out errors to console log .vs. app insights.
    # NOTE: In production environment, you should consider logging this to Azure
    #       application insights.
    logger.error(f"Unhandled error in bot: {str(error)}")
    traceback.print_exc()

    # Don't send error messages to users - just log the error
    # This prevents the "bot encountered an error" message from appearing
    logger.info("Error logged but not shown to user to avoid confusion")


ADAPTER.on_turn_error = on_error

# Initialize Databricks client with error handling
def get_databricks_client():
    """Get Databricks WorkspaceClient with proper error handling"""
    try:
        # Debug environment variable loading
        logger.info(f"Loading Databricks configuration...")
        logger.info(f"DATABRICKS_HOST: {CONFIG.DATABRICKS_HOST}")
        logger.info(f"DATABRICKS_TOKEN present: {bool(CONFIG.DATABRICKS_TOKEN)}")
        logger.info(f"DATABRICKS_TOKEN length: {len(CONFIG.DATABRICKS_TOKEN) if CONFIG.DATABRICKS_TOKEN else 0}")
        
        if not CONFIG.DATABRICKS_TOKEN:
            raise ValueError("DATABRICKS_TOKEN environment variable is not set")
        
        client = WorkspaceClient(
            host=CONFIG.DATABRICKS_HOST, 
            token=CONFIG.DATABRICKS_TOKEN
        )
        logger.info("Databricks client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"Failed to initialize Databricks client: {str(e)}")
        raise

# Initialize clients
workspace_client = get_databricks_client()
genie_api = GenieAPI(workspace_client.api_client)


async def ask_genie(
    question: str, space_id: str, user_session: UserSession, conversation_id: Optional[str] = None
) -> tuple[str, str, str]:
    try:
        # Add user context to the question for better tracking in Databricks
        contextual_question = f"[{user_session.name}] {question}"
        
        loop = asyncio.get_running_loop()
        if conversation_id is None:
            # Start a new conversation
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, contextual_question
            )
            conversation_id = initial_message.conversation_id
        else:
            # Continue existing conversation with a new message
            initial_message = await loop.run_in_executor(
                None, genie_api.create_message_and_wait, space_id, conversation_id, contextual_question
            )
           
        query_result = None
        if initial_message.query_result is not None:
            query_result = await loop.run_in_executor(
                None,
                genie_api.get_message_attachment_query_result,
                #genie_api.get_message_query_result,
                space_id,
                initial_message.conversation_id,
                initial_message.message_id,
                initial_message.attachments[0].attachment_id,
           )
        message_content = await loop.run_in_executor(
            None,
            genie_api.get_message,
            space_id,
            initial_message.conversation_id,
            initial_message.message_id,
        )
        if query_result and query_result.statement_response:
            results = await loop.run_in_executor(
                None,
                workspace_client.statement_execution.get_statement,
                query_result.statement_response.statement_id,
            )

            query_description = ""
            for attachment in message_content.attachments:
                if attachment.query and attachment.query.description:
                    query_description = attachment.query.description
                    break

            return (
                json.dumps(
                    {
                        "columns": results.manifest.schema.as_dict(),
                        "data": results.result.as_dict(),
                        "query_description": query_description,
                    }
                ),
                conversation_id,
                initial_message.message_id,
            )

        if message_content.attachments:
            for attachment in message_content.attachments:
                if attachment.text and attachment.text.content:
                    return (
                        json.dumps({"message": attachment.text.content}),
                        conversation_id,
                        initial_message.message_id,
                    )

        return json.dumps({"message": message_content.content}), conversation_id, initial_message.message_id
    except Exception as e:
        error_str = str(e).lower()  # Convert to lowercase for case-insensitive matching
        error_original = str(e)  # Keep original for logging
        logger.error(f"Error in ask_genie for user {user_session.get_display_name()}: {error_original}")
        
        # Check for IP ACL blocking - look for "blocked" + "ip acl" pattern
        # Error message format: "Source IP address: X.X.X.X is blocked by Databricks IP ACL for workspace"
        if "ip acl" in error_str and "blocked" in error_str:
            logger.error(f"IP ACL blocking detected: {error_original}")
            return (
                json.dumps({
                    "error": "⚠️ **IP Access Blocked**\n\n"
                            "The bot's IP address is blocked by Databricks Account IP Access Control Lists (ACLs).\n\n"
                            "**Administrator Action Required:**\n"
                            "Please check the TROUBLESHOOTING.md documentation for instructions on adding "
                            "the bot's IP address to your Databricks Account IP allow list."
                }),
                conversation_id,
                None,
            )
        
        # Generic error for other cases
        return (
            json.dumps({"error": "An error occurred while processing your request."}),
            conversation_id,
            None,
        )


def process_query_results(answer_json: Dict) -> str:
    response = ""
    if "query_description" in answer_json and answer_json["query_description"]:
        response += f"\n\n{answer_json['query_description']}\n\n"

    if "columns" in answer_json and "data" in answer_json:
        response += "\n"
        columns = answer_json["columns"]
        data = answer_json["data"]
        if isinstance(columns, dict) and "columns" in columns:
            header = "| " + " | ".join(col["name"] for col in columns["columns"]) + " |"
            separator = "|" + "|".join(["---" for _ in columns["columns"]]) + "|"
            response += header + "\n" + separator + "\n"
            for row in data["data_array"]:
                formatted_row = []
                for value, col in zip(row, columns["columns"]):
                    if value is None:
                        formatted_value = "NULL"
                    elif col["type_name"] in ["DECIMAL", "DOUBLE", "FLOAT"]:
                        formatted_value = f"{float(value):,.2f}"
                    elif col["type_name"] in ["INT", "BIGINT", "LONG"]:
                        formatted_value = f"{int(value):,}"
                    else:
                        formatted_value = str(value)
                    formatted_row.append(formatted_value)
                response += "| " + " | ".join(formatted_row) + " |\n"
        else:
            response += f"Unexpected column format: {columns}\n\n"
    elif "error" in answer_json:
        response += f"{answer_json['error']}\n\n"
    elif "message" in answer_json:
        response += f"{answer_json['message']}\n\n"
    else:
        response += "No data available.\n\n"

    return response


class MyBot(ActivityHandler):
    def __init__(self):
        self.user_sessions: Dict[str, UserSession] = {}  # Maps Teams user ID to UserSession
        self.message_feedback: Dict[str, Dict] = {}  # Track feedback for each message

    async def get_or_create_user_session(self, turn_context: TurnContext) -> UserSession:
        """Get or create a user session based on Teams user information"""
        user_id = turn_context.activity.from_property.id
        
        # Check if we already have a session for this user
        if user_id in self.user_sessions:
            session = self.user_sessions[user_id]
            
            # Check if conversation has timed out (4 hours)
            if self._is_conversation_timed_out(session):
                logger.info(f"Conversation timed out for user {session.get_display_name()}, resetting conversation")
                # Reset conversation ID and user context to start fresh
                session.conversation_id = None
                session.user_context.pop('last_conversation_id', None)
                # Update activity time
                session.update_activity()
                return session
            else:
                # Update activity time for active session
                session.update_activity()
                return session
        
        user_name = getattr(turn_context.activity.from_property, 'name', None) or "Usuário"
        session = UserSession(user_id, user_name)
        
        self.user_sessions[user_id] = session
        logger.info(f"Sessão automática criada para: {user_name}")
        
        return session 

    def _is_conversation_timed_out(self, user_session: UserSession) -> bool:
        """Check if conversation has timed out (4 hours)"""
        if not user_session:
            return False
        
        time_since_last_activity = datetime.now(timezone.utc) - user_session.last_activity
        timeout_threshold = timedelta(hours=4)
        
        return time_since_last_activity > timeout_threshold

    def _get_sample_questions(self) -> List[str]:
        """Get sample questions from configuration"""
        # Parse sample questions from config (semicolon-delimited)
        questions_str = CONFIG.SAMPLE_QUESTIONS
        if questions_str:
            # Split by semicolon and strip whitespace
            questions = [q.strip() for q in questions_str.split(';') if q.strip()]
            return questions if questions else [
                "Quais dados estão disponíveis?",
                "Você pode explicar o conjunto de dados?",
                "Quais perguntas posso fazer?"
            ]
        else:
            # Fallback default questions
            return [
                "Quais dados estão disponíveis?",
                "Você pode explicar o conjunto de dados?",
                "Quais perguntas posso fazer?"
            ]

    def create_feedback_card(self, message_id: str, user_id: str) -> Dict:
        """Create an Adaptive Card with thumbs up/down feedback buttons"""
        return {
            "type": "AdaptiveCard",
            "version": "1.3",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "Esta resposta foi útil?",
                    "size": "Small",
                    "color": "Default"
                }
            ],
            "actions": [
                {
                    "type": "Action.Submit",
                    "title": "👍",
                    "data": {
                        "action": "feedback",
                        "messageId": message_id,
                        "userId": user_id,
                        "feedback": "positive"
                    }
                },
                {
                    "type": "Action.Submit",
                    "title": "👎",
                    "data": {
                        "action": "feedback",
                        "messageId": message_id,
                        "userId": user_id,
                        "feedback": "negative"
                    }
                }
            ]
        }

    def create_thank_you_card(self) -> Dict:
        """Create a thank you card to replace feedback buttons after submission"""
        return {
            "type": "AdaptiveCard",
            "version": "1.3",
            "body": [
                {
                    "type": "TextBlock",
                    "text": "✅ Obrigado pelo seu feedback!",
                    "size": "Small",
                    "color": "Good"
                }
            ]
        }

    def create_error_card(self, error_message: str) -> Dict:
        """Create an error card to show when feedback submission fails"""
        return {
            "type": "AdaptiveCard",
            "version": "1.3",
            "body": [
                {
                    "type": "TextBlock",
                    "text": f"❌ {error_message}",
                    "size": "Small",
                    "color": "Attention"
                }
            ]
        }

    async def on_message_activity(self, turn_context: TurnContext):
        # Debug logging for all message activities
        logger.info(f"Message activity type: {turn_context.activity.type}")
        logger.info(f"Message activity name: {turn_context.activity.name}")
        logger.info(f"Message activity value: {turn_context.activity.value}")
        logger.info(f"Message activity text: {turn_context.activity.text}")
        
        # Handle cases where text might be None (e.g., adaptive card interactions)
        if not turn_context.activity.text:
            # Check if this is an adaptive card button click
            if turn_context.activity.value and isinstance(turn_context.activity.value, dict):
                action = turn_context.activity.value.get("action")
                if action == "feedback":
                    logger.info("Detected adaptive card feedback button click in message activity")
                    # Handle as feedback submission
                    try:
                        message_id = turn_context.activity.value.get("messageId")
                        user_id = turn_context.activity.value.get("userId")
                        feedback = turn_context.activity.value.get("feedback")
                        
                        if not all([message_id, user_id, feedback]):
                            logger.error("Missing required feedback data in message activity")
                            return
                        
                        # Store feedback data
                        feedback_key = f"{user_id}_{message_id}"
                        user_session = self.user_sessions.get(user_id)
                        self.message_feedback[feedback_key] = {
                            "message_id": message_id,
                            "user_id": user_id,
                            "feedback": feedback,
                            "conversation_id": user_session.conversation_id if user_session else None,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "user_session": user_session.to_dict() if user_session else None
                        }
                        
                        # Send feedback to Databricks Genie API
                        try:
                            await self._send_feedback_to_api(feedback_key, self.message_feedback[feedback_key])
                            
                            # Send thank you message
                            await turn_context.send_activity("✅ Obrigado pelo seu feedback!")
                            
                        except Exception as e:
                            logger.error(f"Failed to send feedback to Genie API: {str(e)}")
                            await turn_context.send_activity("❌ Falha ao enviar feedback. Por favor, tente novamente.")
                        
                        return
                        
                    except Exception as e:
                        logger.error(f"Error handling feedback in message activity: {str(e)}")
                        return
            
            logger.info("Received message activity without text content, skipping")
            return
            
        question = turn_context.activity.text.strip()
        user_id = turn_context.activity.from_property.id
        
        # Get or create user session
        user_session = await self.get_or_create_user_session(turn_context)
        
        # If we couldn't create a session (no email), ask user to identify themselves
        if not user_session:
            await self._handle_user_identification(turn_context, question)
            return
        
        # Handle special commands first (before checking for timeout reset)
        if await self._handle_special_commands(turn_context, question, user_session):
            return
        
        # Check if conversation was reset due to timeout (only for data questions, not commands)
        if user_session.conversation_id is None and user_session.user_id in self.user_sessions:
            # This means the conversation was reset due to timeout
            await turn_context.send_activity(
                "⏰ **Conversa Reiniciada**\n\n"
                "Sua conversa anterior expirou (mais de 4 horas de inatividade). "
                "Iniciando um novo contexto de conversa.\n\n"
                "Estou processando sua resposta agora!"
            )
        
        # Process the message with user context
        try:
            answer, new_conversation_id, genie_message_id = await ask_genie(
                question, CONFIG.DATABRICKS_SPACE_ID, user_session, user_session.conversation_id
            )
            
            # Update user session with new conversation ID and store the specific message ID for feedback
            user_session.conversation_id = new_conversation_id
            user_session.user_context['last_question'] = question
            user_session.user_context['last_response_time'] = datetime.now(timezone.utc).isoformat()
            user_session.user_context['last_genie_message_id'] = genie_message_id

            answer_json = json.loads(answer)
            response = process_query_results(answer_json)
            
            # Add user context to response
            response = f"**👤 {user_session.name}**\n\n{response}"

            # Send the main response
            await turn_context.send_activity(response)
            
            # Send feedback card as a separate message
            await self._send_feedback_card(turn_context, user_session)
            
        except json.JSONDecodeError:
            await turn_context.send_activity(
                f"**👤 {user_session.name}**\n\n❌ Failed to decode response from the server."
            )
            # Send feedback card for error responses too
            await self._send_feedback_card(turn_context, user_session)
        except Exception as e:
            logger.error(f"Error processing message for {user_session.get_display_name()}: {str(e)}")
            await turn_context.send_activity(
                f"**👤 {user_session.name}**\n\n❌ An error occurred while processing your request."
            )
            # Send feedback card for error responses too
            await self._send_feedback_card(turn_context, user_session)

    async def _handle_user_identification(self, turn_context: TurnContext, question: str):
        """Handle cases where user email is not available"""
        user_id = turn_context.activity.from_property.id
        
        if question.lower() in ["help", "/help", "commands", "/commands"]:
            help_message = f"""🤖 **Informações do Databricks Genie Bot**
**O que eu faço:**
Sou um bot do Teams que se conecta a um Databricks Genie Space, permitindo que você interaja com seus dados usando linguagem natural diretamente no Teams.

**Como eu funciono:**
• Eu me conecto ao workspace Databricks usando as credenciais configuradas
• O contexto da sua conversa é mantido entre as sessões para continuidade
• Eu lembro do histórico da nossa conversa para dar respostas de acompanhamento melhores

**Gerenciamento de Sessão:**
• As conversas reiniciam automaticamente após **4 horas** de inatividade
• Você pode reiniciar manualmente a qualquer momento digitando `reset` ou `new chat`

**Comandos Disponíveis:**
• `help` - Mostra informações detalhadas do bot
• `info` - Ajuda a utilizar o bot
• `reset` - Inicia uma nova conversa
• `logout` - Limpa sua sessão
"""

    async def _handle_special_commands(self, turn_context: TurnContext, question: str, user_session: UserSession) -> bool:
        """Handle special commands. Returns True if command was handled."""
        
        # Special emulator command for setting identity
        if question.lower().startswith("/setuser ") and turn_context.activity.channel_id == "emulator":
            # Format: /setuser john.doe@company.com John Doe
            parts = question.split(" ", 1)
            if len(parts) >= 2:
                #email = parts[1]
                #name = parts[1] if len(parts) > 2 else email.split('@')[0]
                new_name = parts[1]
                user_session.name = new_name
                # Update existing session or create new one
                user_id = turn_context.activity.from_property.id
                session = UserSession(user_id, new_name)
                self.user_sessions[user_id] = session
                #self.email_sessions[email] = session
                
                await turn_context.send_activity(
                    f"✅ **Identity Updated!**\n\n"
                    f"**Name:** {session.name}\n"
                    #f"**Email:** {session.email}\n\n"
                    f"You can now ask me questions about your data!"
                )
                return True
            else:
                await turn_context.send_activity(
                    "❌ **Invalid format**\n\n"
                    "Use: `/setuser Your Name`\n"
                    "Example: `/setuser John Doe`"
                )
                return True

        # Info command
        if question.lower() in ["info", "/info"]:
            is_emulator = turn_context.activity.channel_id == "emulator"
            
            # Mudança: Usamos apenas .name para não depender de e-mail
            info_text = f"""
🤖 **Comandos do Databricks Genie Bot**
**👤 Usuário:** {user_session.name}

**Iniciar Nova Conversa:**
- `reset` ou `new chat`

**Comandos de Usuário:**
- `help` - Mostrar informações detalhadas do bot
- `logout` - Limpar sua sessão (você será reidentificado automaticamente na próxima mensagem)
"""

            # Se estiver no Emulator, mostramos o comando novo de trocar nome
            if is_emulator:
                info_text += """
                **🔧 Emulator Testing Commands:**
                - `/setname Your Name` - Change your display name
                - Example: `/setname John Doe`"""

            info_text += f"""
**Uso Geral:**
- Pergunte qualquer coisa sobre seus dados
- Eu lembrarei do contexto da nossa conversa
- Use os comandos acima para reiniciar quando necessário

**Status Atual:** {"Nova conversa" if user_session.conversation_id is None else "Continuando conversa existente"}
            """
            
            await turn_context.send_activity(info_text)
            return True

        # Logout command
        if question.lower() in ["logout", "/logout", "sign out", "disconnect"]:
            # Clear user session
            user_id = user_session.user_id

            if user_id in self.user_sessions:
                del self.user_sessions[user_id]

            await turn_context.send_activity(
                f"👋 **Até logo, {user_session.name}!**\n\n"
                "Sua sessão foi limpa. Você será reidentificado automaticamente ao enviar sua próxima mensagem."
            )
            return True

        # Help command
        if question.lower() in ["help", "/help", "commands", "/commands", "information", "about", "what is this"]:
            help_message = f"""
🤖 **Informações do Databricks Genie Bot**
**O que eu faço:**
Sou um bot do Teams que se conecta a um Databricks Genie Space, permitindo que você interaja com seus dados usando linguagem natural diretamente no Teams.

**Como eu funciono:**
• Eu me conecto ao workspace Databricks usando as credenciais configuradas
• O contexto da sua conversa é mantido entre as sessões para continuidade
• Eu lembro do histórico da nossa conversa para dar respostas de acompanhamento melhores

**Gerenciamento de Sessão:**
• As conversas reiniciam automaticamente após **4 horas** de inatividade
• Você pode reiniciar manualmente a qualquer momento digitando `reset` ou `new chat`

**Comandos Disponíveis:**
• `help` - Mostra informações detalhadas do bot
• `info` - Ajuda a utilizar o bot
• `reset` - Inicia uma nova conversa
• `logout` - Limpa sua sessão
            """
            
            await turn_context.send_activity(help_message)
            return True

        # New conversation triggers
        new_conversation_triggers = [
            "new conversation", "new chat", "start over", "reset", "clear conversation",
            "/new", "/reset", "/clear", "/start", "begin again", "fresh start"
        ]
        
        if question.lower() in [trigger.lower() for trigger in new_conversation_triggers]:
            user_session.conversation_id = None
            user_session.user_context.pop('last_conversation_id', None)
            await turn_context.send_activity(
                    f"🔄 **Iniciando uma nova conversa, {user_session.name}!**\n\n"
                    "Você pode me perguntar qualquer coisa sobre seus dados."
            )
            return True

        return False

    async def on_invoke_activity(self, turn_context: TurnContext) -> InvokeResponse:
        """Handle invoke activities (like adaptive card button clicks)"""
        try:
            logger.info(f"Received invoke activity: {turn_context.activity.name}")
            logger.info(f"Invoke activity value: {turn_context.activity.value}")
            
            # Check if this is an adaptive card invoke
            if turn_context.activity.name == "adaptiveCard/action":
                invoke_value = turn_context.activity.value
                logger.info(f"Processing adaptive card invoke with value: {invoke_value}")
                return await self.on_adaptive_card_invoke(turn_context, invoke_value)
            
            # Handle other invoke activities if needed
            logger.info(f"Unhandled invoke activity type: {turn_context.activity.name}")
            return InvokeResponse(status_code=200, body="OK")
            
        except Exception as e:
            logger.error(f"Error handling invoke activity: {str(e)}")
            return InvokeResponse(status_code=500, body="Error processing invoke activity")

    async def on_adaptive_card_invoke(self, turn_context: TurnContext, invoke_value: Dict) -> InvokeResponse:
        """Handle Adaptive Card button clicks (feedback submission)"""
        try:
            action = invoke_value.get("action")
            
            if action == "feedback":
                message_id = invoke_value.get("messageId")
                user_id = invoke_value.get("userId")
                feedback = invoke_value.get("feedback")
                
                if not all([message_id, user_id, feedback]):
                    return InvokeResponse(status_code=400, body="Missing required feedback data")
                
                # Store feedback data
                feedback_key = f"{user_id}_{message_id}"
                user_session = self.user_sessions.get(user_id)
                self.message_feedback[feedback_key] = {
                    "message_id": message_id,
                    "user_id": user_id,
                    "feedback": feedback,
                    "conversation_id": user_session.conversation_id if user_session else None,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "user_session": user_session.to_dict() if user_session else None
                }
                
                # Send feedback to Databricks Genie API
                try:
                    await self._send_feedback_to_api(feedback_key, self.message_feedback[feedback_key])
                    
                    # Return updated card with thank you message
                    updated_card = self.create_thank_you_card()
                    
                    return InvokeResponse(
                        status_code=200,
                        body={
                            "type": "AdaptiveCard",
                            "version": "1.3",
                            "body": updated_card["body"]
                        }
                    )
                except Exception as e:
                    logger.error(f"Failed to send feedback to Genie API: {str(e)}")
                    
                    # Return error card
                    error_card = self.create_error_card("Falha ao enviar feedback. Por favor, tente novamente.")
                    
                    return InvokeResponse(
                        status_code=200,
                        body={
                            "type": "AdaptiveCard",
                            "version": "1.3",
                            "body": error_card["body"]
                        }
                    )
            
            return InvokeResponse(status_code=400, body="Unknown action")
            
        except Exception as e:
            logger.error(f"Error handling adaptive card invoke: {str(e)}")
            return InvokeResponse(status_code=500, body="Error processing feedback")

    async def _send_feedback_to_api(self, feedback_key: str, feedback_data: Dict):
        """Send feedback to Databricks Genie send message feedback API"""
        try:
            logger.info(f"Feedback received: {feedback_data}")
            
            # Check if Genie feedback API is enabled
            if not CONFIG.ENABLE_GENIE_FEEDBACK_API:
                logger.info("Genie feedback API is disabled, skipping API call")
                return
            
            # Extract the message ID and user session info
            message_id = feedback_data.get("message_id")
            user_id = feedback_data.get("user_id")
            feedback_type = feedback_data.get("feedback")
            user_session_data = feedback_data.get("user_session")
            
            if not all([message_id, user_id, feedback_type]):
                logger.error(f"Missing required feedback data: {feedback_data}")
                return
            
            # Get the user session to access conversation_id
            user_session = self.user_sessions.get(user_id)
            if not user_session or not user_session.conversation_id:
                logger.error(f"No active conversation found for user {user_id}")
                return
            
            # Convert feedback type to Genie API format
            # positive -> POSITIVE, negative -> NEGATIVE
            genie_feedback_type = "POSITIVE" if feedback_type == "positive" else "NEGATIVE"
            
            # Call the Databricks Genie send message feedback API
            logger.info(f"Sending feedback for specific message ID: {message_id} in conversation: {user_session.conversation_id}")
            await self._send_genie_feedback(
                space_id=CONFIG.DATABRICKS_SPACE_ID,
                conversation_id=user_session.conversation_id,
                message_id=message_id,
                feedback_type=genie_feedback_type
            )
            
            logger.info(f"Feedback sent successfully to Genie API for {feedback_key}")
            
        except Exception as e:
            logger.error(f"Error sending feedback to Genie API: {str(e)}")
            raise

    async def _send_genie_feedback(self, space_id: str, conversation_id: str, message_id: str, feedback_type: str):
        """Send feedback to Databricks Genie API"""
        try:
            loop = asyncio.get_running_loop()
            
            # Use the Genie API to send message feedback
            # Note: The exact method name may vary based on the API version
            # This assumes the method is called send_message_feedback
            await loop.run_in_executor(
                None,
                genie_api.send_message_feedback,
                space_id,
                conversation_id,
                message_id,
                feedback_type
            )
            
            logger.info(f"Successfully sent {feedback_type} feedback for message {message_id} in conversation {conversation_id}")
            
        except AttributeError:
            # If send_message_feedback method doesn't exist, try alternative method names
            logger.warning(f"send_message_feedback method not found, trying alternative approach")
            await self._send_genie_feedback_alternative(space_id, conversation_id, message_id, feedback_type)
        except Exception as e:
            logger.error(f"Error calling Genie API for feedback: {str(e)}")
            raise

    async def _send_genie_feedback_alternative(self, space_id: str, conversation_id: str, message_id: str, feedback_type: str):
        """Alternative method to send feedback if the direct API method is not available"""
        try:
            # If the direct API method is not available, we can use the workspace client
            # to make a direct HTTP request to the Genie feedback endpoint
            import aiohttp
            
            # Construct the API endpoint URL
            base_url = CONFIG.DATABRICKS_HOST.rstrip('/')
            api_endpoint = f"{base_url}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/feedback"
            
            # Prepare the request payload
            payload = {
                "rating": feedback_type
            }
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {CONFIG.DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            }
            
            # Make the HTTP request
            logger.info(f"Sending feedback to: {api_endpoint}")
            logger.info(f"Payload: {payload}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(api_endpoint, json=payload, headers=headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logger.info(f"Successfully sent {feedback_type} feedback via HTTP API")
                    else:
                        logger.error(f"Failed to send feedback via HTTP API: {response.status} - {response_text}")
                        raise Exception(f"HTTP {response.status}: {response_text}")
                        
        except Exception as e:
            logger.error(f"Error in alternative feedback method: {str(e)}")
            raise

    async def _get_last_genie_message_id(self, conversation_id: str) -> Optional[str]:
        """Get the last message ID from the Genie conversation"""
        try:
            if not conversation_id:
                return None
                
            loop = asyncio.get_running_loop()
            # Try different method names for listing messages
            try:
                # Try list_conversation_messages first
                messages = await loop.run_in_executor(
                    None,
                    genie_api.list_conversation_messages,
                    CONFIG.DATABRICKS_SPACE_ID,
                    conversation_id,
                )
            except AttributeError:
                try:
                    # Try get_conversation_messages
                    messages = await loop.run_in_executor(
                        None,
                        genie_api.get_conversation_messages,
                        CONFIG.DATABRICKS_SPACE_ID,
                        conversation_id,
                    )
                except AttributeError:
                    # If neither method exists, return None and log a warning
                    logger.warning("No suitable method found for listing Genie conversation messages")
                    return None
            
            # Handle different response types
            if messages:
                logger.info(f"Messages response type: {type(messages)}")
                logger.info(f"Messages response attributes: {dir(messages)}")
                
                # Check if it's a response object with messages property
                if hasattr(messages, 'messages') and messages.messages:
                    logger.info(f"Found {len(messages.messages)} messages in response.messages")
                    # Sort messages by timestamp to get the most recent one
                    try:
                        sorted_messages = sorted(messages.messages, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                        if sorted_messages:
                            latest_message = sorted_messages[0]
                            logger.info(f"Latest message ID: {latest_message.message_id}")
                            return latest_message.message_id
                    except Exception as e:
                        logger.warning(f"Could not sort messages by timestamp: {e}, using last message")
                        return messages.messages[-1].message_id
                # Check if it's a list-like object
                elif hasattr(messages, '__len__') and len(messages) > 0:
                    logger.info(f"Found {len(messages)} messages in response (list-like)")
                    # Sort messages by timestamp to get the most recent one
                    try:
                        sorted_messages = sorted(messages, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                        if sorted_messages:
                            latest_message = sorted_messages[0]
                            logger.info(f"Latest message ID: {latest_message.message_id}")
                            return latest_message.message_id
                    except Exception as e:
                        logger.warning(f"Could not sort messages by timestamp: {e}, using last message")
                        return messages[-1].message_id
                # Check if it's iterable
                elif hasattr(messages, '__iter__'):
                    message_list = list(messages)
                    if message_list:
                        logger.info(f"Found {len(message_list)} messages in response (iterable)")
                        # Sort messages by timestamp to get the most recent one
                        try:
                            sorted_messages = sorted(message_list, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                            if sorted_messages:
                                latest_message = sorted_messages[0]
                                logger.info(f"Latest message ID: {latest_message.message_id}")
                                return latest_message.message_id
                        except Exception as e:
                            logger.warning(f"Could not sort messages by timestamp: {e}, using last message")
                            return message_list[-1].message_id
                else:
                    logger.warning(f"Unable to extract messages from response of type {type(messages)}")
            return None
            
        except Exception as e:
            logger.error(f"Error getting last Genie message ID: {str(e)}")
            return None

    async def _send_feedback_card(self, turn_context: TurnContext, user_session: UserSession):
        """Send a feedback card after a bot response"""
        try:
            # Check if feedback cards are enabled
            if not CONFIG.ENABLE_FEEDBACK_CARDS:
                return
                
            # Use the actual Genie message ID if available, otherwise generate a fallback
            genie_message_id = user_session.user_context.get('last_genie_message_id')
            if genie_message_id:
                message_id = genie_message_id
                logger.info(f"Creating feedback card for specific Genie message ID: {message_id}")
            else:
                # Fallback to generated ID if we don't have the Genie message ID
                message_id = f"msg_{int(datetime.now().timestamp() * 1000)}"
                logger.warning(f"No Genie message ID available for user {user_session.get_display_name()}, using fallback: {message_id}")
            
            # Create feedback card
            feedback_card = self.create_feedback_card(message_id, user_session.user_id)
            
            # Send the card as an attachment
            activity = Activity(
                type=ActivityTypes.message,
                attachments=[{
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": feedback_card
                }]
            )
            
            await turn_context.send_activity(activity)
            
        except Exception as e:
            logger.error(f"Error sending feedback card: {str(e)}")

    async def on_members_added_activity(
        self, members_added: List[ChannelAccount], turn_context: TurnContext
    ):
        ##print("Members added",members_added)
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                # Try to get user information for personalized welcome
                user_session = await self.get_or_create_user_session(turn_context)
                
                welcome_message = f"""
🤖 **Bem-vindo ao Databricks Genie Bot, {user_session.name}!**
Eu posso ajudar você a analisar seus dados usando linguagem natural. Vou lembrar do contexto da nossa conversa para que você possa fazer perguntas de acompanhamento.

**Comandos Rápidos:**
- `help` - Informações detalhadas do bot
- `info` - Mostrar informações do bot
- `reset` - Reiniciar conversa

**Pronto para começar?**
É só me perguntar qualquer coisa sobre seus dados!
                """
                await turn_context.send_activity(welcome_message)


BOT = MyBot()


async def messages(req: Request) -> Response:
    if "application/json" in req.headers["Content-Type"]:
        body = await req.json()
    else:
        return Response(status=415)

    activity = Activity().deserialize(body)
    auth_header = req.headers.get("Authorization", "")

    try:
        # Handle different adapter types
        if hasattr(ADAPTER, 'process'):
            # CloudAdapter
            response = await ADAPTER.process(req, BOT)
            if response:
                return json_response(data=response.body, status=response.status)
            return Response(status=201)
        else:
            # BotFrameworkAdapter - use process_activity with correct signature
            response = await ADAPTER.process_activity(activity, auth_header, BOT.on_turn)
            if response:
                return json_response(data=response.body, status=response.status)
            return Response(status=201)
    except Exception as e:
        logger.error(f"Error processing request: {str(e)}")
        return Response(status=500)


def init_func(argv):
    APP = web.Application(middlewares=[aiohttp_error_middleware])
    APP.router.add_post("/api/messages", messages)
    return APP


if __name__ == "__main__":
    APP = init_func(None)
    try:
        HOST = "0.0.0.0"
        PORT = int(os.environ.get("PORT", CONFIG.PORT))
        web.run_app(APP, host=HOST, port=PORT)
    except Exception as error:
        raise error
