# Databricks Genie Bot

## Objective

This project implements an experimental chatbot that interacts with Databricks' Genie API, which is currently in Private Preview and not officially supported (will be updated if it changes). The bot is designed to facilitate conversations with Genie, Databricks' AI assistant, through a chat interface like MS Teams.

## Overview

This experimental code creates a Genie BOT in Databricks using the Genie API. It's important to note that this is not production-ready code and is not associated with or endorsed by any employer. The code is intended to be used as-is for experimental and learning purposes only.

## Key Features

- Integrates with Databricks' Genie API to start conversations and process follow-up messages
- Handles user queries and presents Genie's responses
- Manages conversation state for multiple users
- Formats and displays query results in a readable markdown table
- Handles clarification requests from Genie
- **NEW**: Integrated thumbs up/thumbs down feedback system that sends feedback directly to Databricks Genie API
- Real-time feedback collection with proper error handling and user notifications

## Implementation Details

The bot is built using:
- Python
- Bot Framework SDK
- aiohttp for asynchronous HTTP requests
- Databricks Genie API (Public Preview)

The main components of the system are:
- A `ask_genie` function that handles the communication with the Genie API
- A `MyBot` class that processes incoming messages and manages user conversations
- An aiohttp web application that serves as the entry point for bot messages
- **NEW**: Integrated feedback system that sends user feedback directly to Databricks Genie API using the send message feedback endpoint

## Session Management Architecture

### How Authentication Works

The bot uses a **single Databricks token** (configured via `DATABRICKS_TOKEN`) to authenticate all requests to the Databricks Genie API. This is a shared credential that the bot uses on behalf of all users.

### How User Sessions Are Managed

While a single token is used for API authentication, the bot maintains **separate session contexts for each user**:

1. **User Identification**: Each user is identified by their email address (manually provided or from Teams profile)
2. **Session Isolation**: Each user gets their own `UserSession` object that tracks:
   - User email and name
   - Individual conversation ID with Genie
   - Conversation history and context
   - Last activity timestamp
   - User-specific preferences

3. **Conversation Context**: The bot maintains separate Genie conversation threads for each user, ensuring that:
   - User A's questions don't affect User B's conversation
   - Each user can have follow-up questions in their own context
   - Conversations are preserved for 4 hours of inactivity before auto-reset

4. **Query Logging**: When queries are sent to Genie, the user's email is prepended to the question for tracking purposes in Databricks:
   ```
   [user@company.com] What are the top selling products?
   ```

### Benefits of This Architecture

- **Simplified Administration**: Single token to manage instead of per-user credentials
- **User Privacy**: Each user's conversation is isolated from others
- **Context Preservation**: Users can have natural, multi-turn conversations with Genie
- **Audit Trail**: All queries are logged with user attribution in Databricks
- **Scalability**: Can support many concurrent users with minimal configuration

### Security Considerations

- The Databricks token should have appropriate permissions for the Genie Space being accessed
- User emails are used for logging only and are not used for authentication
- Sessions are stored in memory and are cleared after 4 hours of inactivity
- Consider using Azure Key Vault for storing the Databricks token in production

### Other Authentication Options

-While this bot uses a single token to manage many users, you could connect Databricks and Teams via OAuth if you so choose. This is just the example we built

## Disclaimer

This code is experimental and uses a Public Preview API that is not yet supported by Databricks. It should not be used in production environments and is provided strictly for educational and experimental purposes. Use at your own risk.

The code was tested in Azure Bot Framework that facilitates to integrate with any chatbot like MS Teams.

## Setup and Usage

0. Python version 3.13
1. Install the required dependencies listed in `requirements.txt`
2. Set up the necessary environment variables (see Environment Variables section below)
3. Run the `app.py` script to start the bot
4. Call the bot endpoint via Azure Bot Framework or deploy it on a web application to handle the calls.

## Environment Variables

The application uses environment variables for configuration. You can set these in your deployment environment (GitHub Actions, Azure, etc.) or create a `.env` file for local development.

Copy `env.example` to `.env` and fill in your values:

```bash
cp env.example .env
```

### Required Environment Variables

- `DATABRICKS_TOKEN`: Your Databricks personal access token (required)

### Production Environment Variables

- `APP_ID`: Your Azure Bot Service Application ID
- `APP_PASSWORD`: Your Azure Bot Service Application Password
- `APP_TENANTID`: Your Azure Tenant ID

### Optional Environment Variables

- `PORT`: Port number for the application (default: 3978)
- `APP_TYPE`: Bot type - "SingleTenant" or "MultiTenant" (default: "SingleTenant")
- `DATABRICKS_SPACE_ID`: The Databricks Space ID where Genie conversations will take place (has default)
- `DATABRICKS_HOST`: Your Databricks workspace URL (has default)
- `SAMPLE_QUESTIONS`: Semicolon-delimited list of sample questions to show users when they first log in. Customize these for your specific Genie space and use case (default: generic questions about data availability)
- `ADMIN_CONTACT_EMAIL`: Email address displayed to users in the info command for support inquiries (default: admin@company.com)
- `ENABLE_FEEDBACK_CARDS`: Enable/disable feedback collection (default: True)
- `ENABLE_GENIE_FEEDBACK_API`: Enable/disable sending feedback to Databricks Genie API (default: True)

Please refer to the code comments for more detailed information on each component's functionality.

## Feedback System

The bot now includes an integrated feedback system that allows users to provide thumbs up/thumbs down feedback on Genie responses. This feedback is sent directly to the Databricks Genie API using the send message feedback endpoint.

### How it works:

1. **Automatic Feedback Cards**: After each Genie response, users see a feedback card with thumbs up (👍) and thumbs down (👎) buttons
2. **Real-time Submission**: When users click feedback buttons, the feedback is immediately sent to the Databricks Genie API
3. **Error Handling**: If the API call fails, users see an error message and can try again
4. **Configuration**: The feedback system can be enabled/disabled via environment variables

### Feedback Data Sent to Genie API:

- **Message ID**: The actual Genie message ID from the conversation
- **Conversation ID**: The Genie conversation ID
- **Feedback Type**: `POSITIVE` or `NEGATIVE`
- **Space ID**: The Databricks Space ID where the conversation took place

**API Request Format:**
```json
{
  "rating": "POSITIVE"
}
```
or
```json
{
  "rating": "NEGATIVE"
}
```

### Configuration Options:

- `ENABLE_FEEDBACK_CARDS`: Controls whether feedback cards are shown to users
- `ENABLE_GENIE_FEEDBACK_API`: Controls whether feedback is sent to the Genie API
- Both options default to `True` for full functionality

## Customizing Sample Questions

When users first log in, the bot shows them sample questions they can ask about their data. You can customize these questions to match your specific Genie space and use case.

### How to Customize:

1. **Via Environment Variable**: Set the `SAMPLE_QUESTIONS` environment variable with semicolon-delimited questions:
   ```bash
   SAMPLE_QUESTIONS=What are the top selling products this month?;Show me customer churn rates;What's our revenue trend?
   ```

2. **Via .env file**: Add or update the line in your `.env` file:
   ```
   SAMPLE_QUESTIONS=What are the top selling products this month?;Show me customer churn rates;What's our revenue trend?
   ```

3. **In config.py**: The default questions are defined in `config.py` if no environment variable is set

### Examples:

**For Sales Data:**
```
SAMPLE_QUESTIONS=What are the top selling products this month?;Show me revenue by region;Who are our top customers by revenue?
```

**For Support Tickets:**
```
SAMPLE_QUESTIONS=What are the different statuses of tickets and their counts?;Who are my top performing agents?;What's the average resolution time?
```

**For IoT/Sensor Data:**
```
SAMPLE_QUESTIONS=Show me temperature trends over the last 24 hours;What sensors are reporting anomalies?;What's the average uptime by device type?
```

**For Formula 1/Sports Data:**
```
SAMPLE_QUESTIONS=What are the current team standings this year?;Which driver has the most wins?;Show me qualifying positions for the last 10 races
```

The bot will display these questions to users when they first log in, making it easy for them to get started with relevant queries.

## Integrating with MS Teams

```mermaid
sequenceDiagram
    participant User
    participant Azure Portal
    participant GitHub
    participant Azure CLI
    participant VSCode
    participant Web App
    participant Bot Service
    participant Databricks
    participant Teams

    User->>GitHub: Clone DatabricksGenieBOT repository
    User->>Azure Portal: Create App Service Plan (Linux)
    User->>Azure Portal: Create Web App (Python 3.12)
    User->>Azure Portal: Create Azure Bot AI Service
    User->>Azure Portal: Configure Bot (Secret, Icon, Description)
    User->>Azure Portal: Configure Bot Messaging Endpoint
    User->>Azure Portal: Configure Bot Channel (Teams)
    User->>Databricks: Curate Genie Space and get Space ID
    User->>Databricks: Generate Token
    User->>Azure Portal: Configure Web App (Startup Command, Environment Variables)
    User->>VSCode: Open project folder
    User->>Azure CLI: Install Azure CLI
    User->>VSCode: Create and activate virtual environment
    User->>VSCode: Install requirements
    User->>Azure CLI: Login (az login)
    User->>Azure CLI: Deploy Web App (az webapp up)
    User->>Azure Portal: Configure Web App settings
    User->>Azure Portal: Test Bot in Web Chat
    User->>Teams: Test Bot in Microsoft Teams
