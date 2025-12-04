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
    """Representa uma sessão de usuário"""
    def __init__(self, user_id: str, name: str = None):
        self.user_id = user_id  # Teams user ID
        self.name = name or "Usuario"
        self.conversation_id = None
        self.created_at = datetime.now(timezone.utc)
        self.last_activity = datetime.now(timezone.utc)
        self.is_authenticated = True  # Always true for Teams users
        self.user_context = {}
    
    def update_activity(self):
        """Atualize o registro de data e hora da última atividade."""
        self.last_activity = datetime.now(timezone.utc)
    
    def to_dict(self):
        """Converta a sessão em dicionário para registro/debug"""
        return {
            "user_id": self.user_id,
            "name": self.name,
            "conversation_id": self.conversation_id,
            "created_at": self.created_at.isoformat(),
            "last_activity": self.last_activity.isoformat(),
            "is_authenticated": self.is_authenticated
        }
    
    def get_display_name(self):
        """Pega um nome de exibição amigável para o usuário."""
        return f"{self.name} ({self.user_id})"

# Para desenvolvimento local com o Bot Framework Emulator, use BotFrameworkAdapter
if CONFIG.APP_ID and CONFIG.APP_PASSWORD:
    # Produção: Use CloudAdapter
    ADAPTER = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))
else:
    # Teste local: Use BotFrameworkAdapter com credenciais vazias
    SETTINGS = BotFrameworkAdapterSettings("", "")
    ADAPTER = BotFrameworkAdapter(SETTINGS)


async def on_error(context: TurnContext, error: Exception):
    # Esta verificação escreve erros no console em vez de no Application Insights.
    # NOTA: Em ambiente de produção, você deve considerar registrar isso no Azure
    #       Application Insights.
    logger.error(f"Erro inesperado no bot: {str(error)}")
    traceback.print_exc()

    # Não envie mensagens de erro para os usuários - apenas registre o erro
    # Isso evita que a mensagem "bot encontrou um erro" apareça
    logger.info("Erro registrado, mas não mostrado ao usuário para evitar confusão")


ADAPTER.on_turn_error = on_error

# Inicializar o cliente Databricks com tratamento de erros.
def get_databricks_client():
    """Obtenha o WorkspaceClient do Databricks com tratamento adequado de erros"""
    try:
        # Depurar carregamento de variáveis de ambiente
        logger.info(f"Carregando configuração do Databricks...")
        logger.info(f"DATABRICKS_HOST: {CONFIG.DATABRICKS_HOST}")
        logger.info(f"DATABRICKS_TOKEN present: {bool(CONFIG.DATABRICKS_TOKEN)}")
        logger.info(f"DATABRICKS_TOKEN length: {len(CONFIG.DATABRICKS_TOKEN) if CONFIG.DATABRICKS_TOKEN else 0}")
        
        if not CONFIG.DATABRICKS_TOKEN:
            raise ValueError("DATABRICKS_TOKEN variável de ambiente não está definida.")
        
        client = WorkspaceClient(
            host=CONFIG.DATABRICKS_HOST, 
            token=CONFIG.DATABRICKS_TOKEN
        )
        logger.info("Cliente Databricks inicializado com sucesso")
        return client
    except Exception as e:
        logger.error(f"Falha ao inicializar o cliente Databricks: {str(e)}")
        raise

# Inicializar clientes
workspace_client = get_databricks_client()
genie_api = GenieAPI(workspace_client.api_client)


async def ask_genie(
    question: str, space_id: str, user_session: UserSession, conversation_id: Optional[str] = None
) -> tuple[str, str, str]:
    try:
        # Adicionar contexto do usuário à pergunta para melhor rastreamento no Databricks
        contextual_question = f"[{user_session.name}] {question}"
        
        loop = asyncio.get_running_loop()
        if conversation_id is None:
            # Iniciar uma nova conversa
            initial_message = await loop.run_in_executor(
                None, genie_api.start_conversation_and_wait, space_id, contextual_question
            )
            conversation_id = initial_message.conversation_id
        else:
            # Continuar conversa existente com uma nova mensagem
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
        error_str = str(e).lower()  # Converter para minúsculas para correspondência sem distinção entre maiúsculas e minúsculas
        error_original = str(e)  # Manter original para registro
        logger.error(f"Erro em ask_genie para o usuário {user_session.get_display_name()}: {error_original}")
        
    
        if "ip acl" in error_str and "blocked" in error_str:
            logger.error(f"Bloqueio de IP ACL detectado: {error_original}")
            return (
                json.dumps({
                    "error": "⚠️ **Acesso IP Bloqueado**\n\n"
                            "O endereço IP do bot está bloqueado pelas Listas de Controle de Acesso (ACLs) de IP da Conta Databricks.\n\n"
                            "**Ação do Administrador Necessária:**\n"
                            "Verificar a documentação TROUBLESHOOTING.md para instruções sobre como adicionar "
                            "o endereço IP do bot à lista de permissões de IP da sua Conta Databricks."
                }),
                conversation_id,
                None,
            )
        
        # Erro genérico para outros casos
        return (
            json.dumps({"error": "Ocorreu um erro ao processar sua solicitação."}),
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
            response += f"Formato de coluna inesperado: {columns}\n\n"
    elif "error" in answer_json:
        response += f"{answer_json['error']}\n\n"
    elif "message" in answer_json:
        response += f"{answer_json['message']}\n\n"
    else:
        response += "Nenhum dado disponível.\n\n"

    return response


class MyBot(ActivityHandler):
    def __init__(self):
        self.user_sessions: Dict[str, UserSession] = {}  # Mapeia o ID do usuário do Teams para UserSession
        self.message_feedback: Dict[str, Dict] = {}  # Rastreia feedback para cada mensagem

    async def get_or_create_user_session(self, turn_context: TurnContext) -> UserSession:
        """Obter ou criar uma sessão de usuário com base nas informações do usuário do Teams"""
        user_id = turn_context.activity.from_property.id
        
        # Verificar se já temos uma sessão para este usuário
        if user_id in self.user_sessions:
            session = self.user_sessions[user_id]
            
            # Verificar se a conversa expirou (4 horas)
            if self._is_conversation_timed_out(session):
                logger.info(f"Conversation timed out for user {session.get_display_name()}, resetting conversation")
                # Redefinir ID da conversa e contexto do usuário para começar do zero
                session.conversation_id = None
                session.user_context.pop('last_conversation_id', None)
                # Atualizar o tempo de atividade
                session.update_activity()
                return session
            else:
                # Atualizar o tempo de atividade para sessão ativa
                session.update_activity()
                return session
        
        user_name = getattr(turn_context.activity.from_property, 'name', None) or "Usuário"
        session = UserSession(user_id, user_name)
        
        self.user_sessions[user_id] = session
        logger.info(f"Sessão automática criada para: {user_name}")
        
        return session 

    def _is_conversation_timed_out(self, user_session: UserSession) -> bool:
        """Verificar se a conversa expirou (4 horas)"""
        if not user_session:
            return False
        
        time_since_last_activity = datetime.now(timezone.utc) - user_session.last_activity
        timeout_threshold = timedelta(hours=4)
        
        return time_since_last_activity > timeout_threshold

    def _get_sample_questions(self) -> List[str]:
        """Obter perguntas de exemplo da configuração"""
        # Analisa as perguntas de exemplo da configuração (delimitadas por ponto e vírgula)
        questions_str = CONFIG.SAMPLE_QUESTIONS
        if questions_str:
            # Divide por ponto e vírgula e remove espaços em branco (nas bordas)
            questions = [q.strip() for q in questions_str.split(';') if q.strip()]
            return questions if questions else [
                "Quais dados estão disponíveis?",
                "Você pode explicar o conjunto de dados?",
                "Quais perguntas posso fazer?"
            ]
        else:
            # Perguntas padrão de fallback
            return [
                "Quais dados estão disponíveis?",
                "Você pode explicar o conjunto de dados?",
                "Quais perguntas posso fazer?"
            ]

    def create_feedback_card(self, message_id: str, user_id: str) -> Dict:
        """Criar um Adaptive Card com botões de feedback positivo/negativo"""
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
        """Criar um cartão de agradecimento para substituir os botões de feedback após o envio"""
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
        """Criar um cartão de erro para mostrar quando o envio de feedback falhar"""
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
        # Registro de depuração para todas as atividades de mensagem
        logger.info(f"Tipo de atividade de mensagem: {turn_context.activity.type}")
        logger.info(f"Nome da atividade de mensagem: {turn_context.activity.name}")
        logger.info(f"Valor da atividade de mensagem: {turn_context.activity.value}")
        logger.info(f"Texto da atividade de mensagem: {turn_context.activity.text}")
        
        # Lida com casos onde o campo de texto pode ser None (ex: cliques em cartões)
        if not turn_context.activity.text:
            # Verifica se este é um clique em botão de cartão adaptativo
            if turn_context.activity.value and isinstance(turn_context.activity.value, dict):
                action = turn_context.activity.value.get("action")
                if action == "feedback":
                    logger.info("Detectado clique no botão de feedback do cartão adaptativo na atividade de mensagem")
                    # Lida com o envio de feedback
                    try:
                        message_id = turn_context.activity.value.get("messageId")
                        user_id = turn_context.activity.value.get("userId")
                        feedback = turn_context.activity.value.get("feedback")
                        
                        if not all([message_id, user_id, feedback]):
                            logger.error("Dados de feedback obrigatórios ausentes na atividade de mensagem")
                            return
                        
                        # Armazenar dados de feedback
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
                        
                        # Enviar feedback para a API Databricks Genie
                        try:
                            await self._send_feedback_to_api(feedback_key, self.message_feedback[feedback_key])
                            
                            # Enviar mensagem de agradecimento
                            await turn_context.send_activity("✅ Obrigado pelo seu feedback!")
                            
                        except Exception as e:
                            logger.error(f"Falha ao enviar feedback para a API Genie: {str(e)}")
                            await turn_context.send_activity("❌ Falha ao enviar feedback. Por favor, tente novamente.")
                        
                        return
                        
                    except Exception as e:
                        logger.error(f"Erro ao lidar com feedback na atividade de mensagem: {str(e)}")
                        return
            
            logger.info("Atividade de mensagem recebida sem conteúdo de texto, ignorando")
            return
            
        question = turn_context.activity.text.strip()
        user_id = turn_context.activity.from_property.id
        
        # Obter ou criar sessão do usuário
        user_session = await self.get_or_create_user_session(turn_context)
        
        # Se não pudermos criar uma sessão, peça ao usuário para se identificar
        if not user_session:
            await self._handle_user_identification(turn_context, question)
            return
        
        # Lida com comandos especiais primeiro (antes de verificar o reset por timeout)
        if await self._handle_special_commands(turn_context, question, user_session):
            return
        
        # Verificar se a conversa foi reiniciada devido ao tempo limite (apenas para perguntas de dados, não comandos)
        if user_session.conversation_id is None and user_session.user_id in self.user_sessions:
            # Isso significa que a conversa foi reiniciada devido ao tempo limite
            await turn_context.send_activity(
                "⏰ **Conversa Reiniciada**\n\n"
                "Sua conversa anterior expirou (mais de 4 horas de inatividade). "
                "Iniciando um novo contexto de conversa.\n\n"
                "Estou processando sua resposta agora!"
            )
        
        # Processa a mensagem mantendo o contexto da conversa
        try:
            answer, new_conversation_id, genie_message_id = await ask_genie(
                question, CONFIG.DATABRICKS_SPACE_ID, user_session, user_session.conversation_id
            )
            
            # Atualizar sessão do usuário com novo ID de conversa e armazenar o ID da mensagem específica para feedback
            user_session.conversation_id = new_conversation_id
            user_session.user_context['last_question'] = question
            user_session.user_context['last_response_time'] = datetime.now(timezone.utc).isoformat()
            user_session.user_context['last_genie_message_id'] = genie_message_id

            answer_json = json.loads(answer)
            response = process_query_results(answer_json)
            
            # Adiciona o contexto do usuário à resposta
            response = f"**👤 {user_session.name}**\n\n{response}"

            # Enviar a resposta principal
            await turn_context.send_activity(response)
            
            # Enviar cartão de feedback como uma mensagem separada
            await self._send_feedback_card(turn_context, user_session)
            
        except json.JSONDecodeError:
            await turn_context.send_activity(
                f"**👤 {user_session.name}**\n\n❌ Falha ao decodificar a resposta do servidor."
            )
            # Enviar cartão de feedback para respostas com erro também
            await self._send_feedback_card(turn_context, user_session)
        except Exception as e:
            logger.error(f"Erro ao processar mensagem para {user_session.get_display_name()}: {str(e)}")
            await turn_context.send_activity(
                f"**👤 {user_session.name}**\n\n❌ Ocorreu um erro ao processar sua solicitação."
            )
            # Enviar cartão de feedback para respostas com erro também
            await self._send_feedback_card(turn_context, user_session)

    async def _handle_user_identification(self, turn_context: TurnContext, question: str):
        """Lida com casos onde o email do usuário não está disponível"""
        user_id = turn_context.activity.from_property.id
        
        if question.lower() in ["help", "/help", "commands", "/commands"]:
            help_message = f"""🤖 **Informações do Bot de Funil de Vendas**
**O que eu faço:**
Sou um bot do Teams que se conecta a um Databricks Genie Space, permitindo que você interaja com seus dados usando linguagem natural diretamente no Teams.

**Como eu funciono:**
• Eu me conecto ao workspace Databricks usando as credenciais configuradas
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
        """Trata comandos especiais. Retorna True se o comando foi processado."""
        
        # Comando especial do emulador para configuração de usuário
        if question.lower().startswith("/setuser ") and turn_context.activity.channel_id == "emulator":
            parts = question.split(" ", 1)
            if len(parts) >= 2:
                new_name = parts[1]
                user_session.name = new_name
                # Update existing session or create new one
                user_id = turn_context.activity.from_property.id
                session = UserSession(user_id, new_name)
                self.user_sessions[user_id] = session
                
                await turn_context.send_activity(
                    f"✅ **Identidade Atualizada!**\n\n"
                    f"**Nome:** {session.name}\n"
                    f"Você agora pode me fazer perguntas sobre seus dados!"
                )
                return True
            else:
                await turn_context.send_activity(
                    "❌ **Formato inválido**\n\n"
                    "Use: `/setuser Seu Nome`\n"
                    "Exemplo: `/setuser João Silva`"
                )
                return True

        # Comando info
        if question.lower() in ["info", "/info"]:
            is_emulator = turn_context.activity.channel_id == "emulator"
            
            # Mudança: Usamos apenas .name para não depender de e-mail
            info_text = f"""
🤖 **Comandos do Bot de Funil de Vendas**
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
                **Comandos de Teste do Emulador:**
                - `/setname Seu Nome` - Altere seu nome de exibição
                - Exemplo: `/setname João Silva`"""

            info_text += f"""
**Uso Geral:**
- Pergunte qualquer coisa sobre seus dados
- Eu lembrarei do contexto da nossa conversa
- Use os comandos acima para reiniciar quando necessário

**Status Atual:** {"Nova conversa" if user_session.conversation_id is None else "Continuando conversa existente"}
            """
            
            await turn_context.send_activity(info_text)
            return True

        # Comando logout
        if question.lower() in ["logout", "/logout", "sign out", "disconnect"]:
            # Limpar sessão do usuário
            user_id = user_session.user_id

            if user_id in self.user_sessions:
                del self.user_sessions[user_id]

            await turn_context.send_activity(
                f"👋 **Até logo, {user_session.name}!**\n\n"
                "Sua sessão foi limpa. Você será reidentificado automaticamente ao enviar sua próxima mensagem."
            )
            return True

        # Comando help
        if question.lower() in ["help", "/help", "commands", "/commands", "information", "about", "what is this"]:
            help_message = f"""
🤖 **Informações do Bot de Funil de Vendas**
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

        # Gatilhos para nova conversa
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
        """Lida com invocações (como cliques em botões de cartões)"""
        try:
            logger.info(f"Invocação de atividade recebida: {turn_context.activity.name}")
            logger.info(f"Valor da atividade de invocação: {turn_context.activity.value}")
            
            # Verifica se esta é uma invocação de cartão adaptativo
            if turn_context.activity.name == "adaptiveCard/action":
                invoke_value = turn_context.activity.value
                logger.info(f"Processando invocação de cartão adaptativo com valor: {invoke_value}")
                return await self.on_adaptive_card_invoke(turn_context, invoke_value)
            
            # Lida com outras atividades de invocação, se necessário
            logger.info(f"Tipo de atividade de invocação não tratado: {turn_context.activity.name}")
            return InvokeResponse(status_code=200, body="OK")
            
        except Exception as e:
            logger.error(f"Erro ao lidar com a invocação de atividade: {str(e)}")
            return InvokeResponse(status_code=500, body="Erro ao processar a atividade de invocação")

    async def on_adaptive_card_invoke(self, turn_context: TurnContext, invoke_value: Dict) -> InvokeResponse:
        """Lida com cliques em botões de Cartão Adaptativo (envio de feedback)"""
        try:
            action = invoke_value.get("action")
            
            if action == "feedback":
                message_id = invoke_value.get("messageId")
                user_id = invoke_value.get("userId")
                feedback = invoke_value.get("feedback")
                
                if not all([message_id, user_id, feedback]):
                    return InvokeResponse(status_code=400, body="Dados de feedback obrigatórios ausentes")
                
                # Armazenar dados de feedback
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
                
                # Enviar feedback para a API do Databricks Genie
                try:
                    await self._send_feedback_to_api(feedback_key, self.message_feedback[feedback_key])
                    
                    # Retornar cartão atualizado com mensagem de agradecimento
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
                    logger.error(f"Falha ao enviar feedback para a API do Genie: {str(e)}")
                    
                    # Retornar cartão de erro
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
        """Envia feedback para a API de feedback de mensagens do Databricks Genie"""
        try:
            logger.info(f"Feedback received: {feedback_data}")
            
            # Verificar se a API de feedback do Genie está habilitada
            if not CONFIG.ENABLE_GENIE_FEEDBACK_API:
                logger.info("API de feedback do Genie está desabilitada, pulando chamada de API")
                return
            
            # Extrair o ID da mensagem e informações da sessão do usuário
            message_id = feedback_data.get("message_id")
            user_id = feedback_data.get("user_id")
            feedback_type = feedback_data.get("feedback")
            user_session_data = feedback_data.get("user_session")
            
            if not all([message_id, user_id, feedback_type]):
                logger.error(f"Missing required feedback data: {feedback_data}")
                return
            
            # Obter a sessão do usuário para acessar conversation_id
            user_session = self.user_sessions.get(user_id)
            if not user_session or not user_session.conversation_id:
                logger.error(f"Nenhuma conversa ativa encontrada para o usuário {user_id}")
                return
            
            # Converter o tipo de feedback para o formato da API do Genie
            # positive -> POSITIVE, negative -> NEGATIVE
            genie_feedback_type = "POSITIVE" if feedback_type == "positive" else "NEGATIVE"
            
            # Chamar a API de feedback de mensagem do Databricks Genie
            logger.info(f"Enviando feedback para o ID da mensagem específica: {message_id} na conversa: {user_session.conversation_id}")
            await self._send_genie_feedback(
                space_id=CONFIG.DATABRICKS_SPACE_ID,
                conversation_id=user_session.conversation_id,
                message_id=message_id,
                feedback_type=genie_feedback_type
            )
            
            logger.info(f"Feedback enviado com sucesso para a API do Genie para {feedback_key}")
            
        except Exception as e:
            logger.error(f"Erro ao enviar feedback para a API do Genie: {str(e)}")
            raise

    async def _send_genie_feedback(self, space_id: str, conversation_id: str, message_id: str, feedback_type: str):
        """Envia feedback para a API do Databricks Genie"""
        try:
            loop = asyncio.get_running_loop()
            
            # Use the Genie API to send feedback for a message
            await loop.run_in_executor(
                None,
                genie_api.send_message_feedback,
                space_id,
                conversation_id,
                message_id,
                feedback_type
            )
            
            logger.info(f"Feedback {feedback_type} enviado com sucesso para a mensagem {message_id} na conversa {conversation_id}")
            
        except AttributeError:
            # Se o método send_message_feedback não existir, tenta nomes de métodos alternativos
            logger.warning(f"Método send_message_feedback não encontrado, tentando abordagem alternativa")
            await self._send_genie_feedback_alternative(space_id, conversation_id, message_id, feedback_type)
        except Exception as e:
            logger.error(f"Erro ao chamar a API do Genie para feedback: {str(e)}")
            raise

    async def _send_genie_feedback_alternative(self, space_id: str, conversation_id: str, message_id: str, feedback_type: str):
        """Método alternativo para enviar feedback se o método direto da API não estiver disponível"""
        try:
            # Se o método direto da API não estiver disponível, podemos usar o cliente do workspace
            # para fazer uma requisição HTTP direta ao endpoint de feedback do Genie
            import aiohttp
            
            # Construir a URL do endpoint da API
            base_url = CONFIG.DATABRICKS_HOST.rstrip('/')
            api_endpoint = f"{base_url}/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/feedback"
            
            # Preparar o payload da requisição
            payload = {
                "rating": feedback_type
            }
            
            # Prepare headers
            headers = {
                "Authorization": f"Bearer {CONFIG.DATABRICKS_TOKEN}",
                "Content-Type": "application/json"
            }
            
            # Faz a requisição HTTP
            logger.info(f"Sending feedback to: {api_endpoint}")
            logger.info(f"Payload: {payload}")
            
            async with aiohttp.ClientSession() as session:
                async with session.post(api_endpoint, json=payload, headers=headers) as response:
                    response_text = await response.text()
                    if response.status == 200:
                        logger.info(f"Feedback {feedback_type} enviado com sucesso via API HTTP")
                    else:
                        logger.error(f"Falha ao enviar feedback via API HTTP: {response.status} - {response_text}")
                        raise Exception(f"HTTP {response.status}: {response_text}")
                        
        except Exception as e:
            logger.error(f"Erro no método alternativo de feedback: {str(e)}")
            raise

    async def _get_last_genie_message_id(self, conversation_id: str) -> Optional[str]:
        """Obter o ID da última mensagem da conversa do Genie"""
        try:
            if not conversation_id:
                return None
                
            loop = asyncio.get_running_loop()
            # Tenta nomes de métodos diferentes para listar mensagens
            try:
                # Tenta list_conversation_messages primeiro
                messages = await loop.run_in_executor(
                    None,
                    genie_api.list_conversation_messages,
                    CONFIG.DATABRICKS_SPACE_ID,
                    conversation_id,
                )
            except AttributeError:
                try:
                    # Tenta get_conversation_messages
                    messages = await loop.run_in_executor(
                        None,
                        genie_api.get_conversation_messages,
                        CONFIG.DATABRICKS_SPACE_ID,
                        conversation_id,
                    )
                except AttributeError:
                    # Se nenhum dos métodos existir, retorna None e registra um aviso
                    logger.warning("Nenhum método adequado encontrado para listar mensagens da conversa do Genie")
                    return None
            
            # Lida com diferentes tipos de resposta
            if messages:
                logger.info(f"Messages response type: {type(messages)}")
                logger.info(f"Messages response attributes: {dir(messages)}")
                
                # Verifica se é um objeto de resposta com a propriedade messages
                if hasattr(messages, 'messages') and messages.messages:
                    logger.info(f"Found {len(messages.messages)} messages in response.messages")
                    # Ordena as mensagens por carimbo de data/hora para obter a mais recente
                    try:
                        sorted_messages = sorted(messages.messages, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                        if sorted_messages:
                            latest_message = sorted_messages[0]
                            logger.info(f"ID da última mensagem: {latest_message.message_id}")
                            return latest_message.message_id
                    except Exception as e:
                        logger.warning(f"Não foi possível ordenar as mensagens por carimbo de data/hora: {e}, usando a última mensagem")
                        return messages.messages[-1].message_id
                # Verifica se é um objeto semelhante a uma lista
                elif hasattr(messages, '__len__') and len(messages) > 0:
                    logger.info(f"Found {len(messages)} messages in response (list-like)")
                    # Ordena as mensagens por carimbo de data/hora para obter a mais recente
                    try:
                        sorted_messages = sorted(messages, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                        if sorted_messages:
                            latest_message = sorted_messages[0]
                            logger.info(f"ID da última mensagem: {latest_message.message_id}")
                            return latest_message.message_id
                    except Exception as e:
                        logger.warning(f"Não foi possível ordenar as mensagens por carimbo de data/hora: {e}, usando a última mensagem")
                        return messages[-1].message_id
                # Verifica se é iterável
                elif hasattr(messages, '__iter__'):
                    message_list = list(messages)
                    if message_list:
                        logger.info(f"Found {len(message_list)} messages in response (iterable)")
                        # Ordena as mensagens por carimbo de data/hora para obter a mais recente
                        try:
                            sorted_messages = sorted(message_list, key=lambda x: getattr(x, 'created_at', 0), reverse=True)
                            if sorted_messages:
                                latest_message = sorted_messages[0]
                                logger.info(f"ID da última mensagem: {latest_message.message_id}")
                                return latest_message.message_id
                        except Exception as e:
                            logger.warning(f"Não foi possível ordenar as mensagens por carimbo de data/hora: {e}, usando a última mensagem")
                            return message_list[-1].message_id
                else:
                    logger.warning(f"Não foi possível extrair mensagens da resposta do tipo {type(messages)}")
            return None
            
        except Exception as e:
            logger.error(f"Erro ao obter o ID da última mensagem do Genie: {str(e)}")
            return None

    async def _send_feedback_card(self, turn_context: TurnContext, user_session: UserSession):
        """Enviar um cartão de feedback após uma resposta do bot"""
        try:
            # Verifica se os cartões de feedback estão habilitados
            if not CONFIG.ENABLE_FEEDBACK_CARDS:
                return
                
            # Use o ID real da mensagem do Genie, se disponível, caso contrário, gere um fallback
            genie_message_id = user_session.user_context.get('last_genie_message_id')
            if genie_message_id:
                message_id = genie_message_id
                logger.info(f"Criando cartão de feedback para o ID específico da mensagem do Genie: {message_id}")
            else:
                # Fallback to generated ID if we don't have the Genie message ID
                message_id = f"msg_{int(datetime.now().timestamp() * 1000)}"
                logger.warning(f"Nenhum ID de mensagem do Genie disponível para o usuário {user_session.get_display_name()}, usando fallback: {message_id}")
            
            # Cria o cartão de feedback
            feedback_card = self.create_feedback_card(message_id, user_session.user_id)
            
            # Envia o cartão como um anexo
            activity = Activity(
                type=ActivityTypes.message,
                attachments=[{
                    "contentType": "application/vnd.microsoft.card.adaptive",
                    "content": feedback_card
                }]
            )
            
            await turn_context.send_activity(activity)
            
        except Exception as e:
            logger.error(f"Erro ao enviar o cartão de feedback: {str(e)}")

    async def on_members_added_activity(
        self, members_added: List[ChannelAccount], turn_context: TurnContext
    ):
        for member in members_added:
            if member.id != turn_context.activity.recipient.id:
                # Tenta obter informações do usuário para uma recepção personalizada
                user_session = await self.get_or_create_user_session(turn_context)
                
                welcome_message = f"""
🤖 **Bem-vindo ao Bot de Funil de Vendas, {user_session.name}!**
Eu posso ajudar você a analisar seus dados usando linguagem natural. Vou lembrar do contexto da nossa conversa para que você possa fazer perguntas de acompanhamento.

**Comandos Rápidos:**
- `help` - Informações detalhadas do bot
- `info` - Mostrar informações do bot
- `reset` - Reiniciar conversa

**Pronto para começar?**
É só me perguntar qualquer coisa sobre seus dados!"""
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
        # Lida com diferentes tipos de adaptadores
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
        logger.error(f"Erro ao processar a requisição: {str(e)}")
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
