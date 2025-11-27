# Microsoft Teams Integration Guide

## Overview
This guide will help you deploy your Databricks Genie Bot to Microsoft Teams. Your bot is already configured with the Bot Framework, but needs additional Teams-specific configuration.

## Prerequisites
- Azure Bot Service created and configured
- Bot deployed to Azure App Service
- Microsoft Teams admin access (for organization-wide deployment)
- Valid Azure Bot Service credentials

## Step 1: Azure Bot Service Configuration

### 1.1 Configure Messaging Endpoint
1. Go to [Azure Portal](https://portal.azure.com)
2. Navigate to your Bot Service resource
3. Go to **Configuration** → **Messaging**
4. Set the messaging endpoint to: `https://your-app-name.azurewebsites.net/api/messages`
5. Save the configuration

### 1.2 Enable Teams Channel
1. In your Bot Service, go to **Channels**
2. Click on **Microsoft Teams** channel
3. Click **Apply** to enable the channel
4. Note the Teams App ID (should match your bot's App ID)

## Step 2: Create Teams App Package

### 2.1 Create Manifest File
You'll need to create a `manifest.json` file for your Teams app. Microsoft provides templates and tools:

**Option 1: Use Teams Developer Portal (Recommended)**
1. Go to [Teams Developer Portal](https://dev.teams.microsoft.com/apps)
2. Click **New app** to create a new app
3. Fill in basic information
4. Go to **Configure** → **App features** → **Bot**
5. Select your existing bot (use your Azure Bot App ID)
6. Configure capabilities: Personal, Team, Group Chat as needed
7. Download the app package when complete

**Option 2: Create Manifest Manually**
Use Microsoft's manifest template: [Teams App Manifest Schema](https://learn.microsoft.com/en-us/microsoftteams/platform/resources/schema/manifest-schema)

Here's a minimal `manifest.json` template for a bot:
```json
{
  "$schema": "https://developer.microsoft.com/json-schemas/teams/v1.16/MicrosoftTeams.schema.json",
  "manifestVersion": "1.16",
  "version": "1.0.0",
  "id": "YOUR-BOT-APP-ID-HERE",
  "packageName": "com.yourcompany.databricksgeniebot",
  "developer": {
    "name": "Your Company Name",
    "websiteUrl": "https://yourcompany.com",
    "privacyUrl": "https://yourcompany.com/privacy",
    "termsOfUseUrl": "https://yourcompany.com/terms"
  },
  "name": {
    "short": "Databricks Genie Bot",
    "full": "Databricks Genie Bot - Data Assistant"
  },
  "description": {
    "short": "AI assistant for Databricks data queries",
    "full": "A Teams bot that connects to Databricks Genie Space, allowing you to interact with your data through natural language queries."
  },
  "icons": {
    "outline": "outline.png",
    "color": "color.png"
  },
  "accentColor": "#FFFFFF",
  "bots": [
    {
      "botId": "YOUR-BOT-APP-ID-HERE",
      "scopes": ["personal", "team", "groupchat"],
      "supportsFiles": false,
      "isNotificationOnly": false,
      "commandLists": [
        {
          "scopes": ["personal", "team", "groupchat"],
          "commands": [
            {
              "title": "help",
              "description": "Show all available commands"
            },
            {
              "title": "info",
              "description": "Show bot information"
            },
            {
              "title": "whoami",
              "description": "Display your user information"
            },
            {
              "title": "reset",
              "description": "Start a fresh conversation"
            }
          ]
        }
      ]
    }
  ],
  "permissions": ["identity", "messageTeamMembers"],
  "validDomains": ["your-app-name.azurewebsites.net"]
}
```

**Important**: Replace the following in the template:
- `YOUR-BOT-APP-ID-HERE` - Your Azure Bot Service App ID
- `com.yourcompany.databricksgeniebot` - Your package name
- `Your Company Name` - Your organization name
- URLs - Your actual company URLs
- `your-app-name.azurewebsites.net` - Your Azure App Service domain

### 2.2 Create App Icons
You need two icon files for your Teams app:

**Icon Requirements:**
- **color.png**: 192x192 pixels, full-color icon, PNG format
- **outline.png**: 32x32 pixels, white icon on transparent background, PNG format

**Resources for Icons:**
- **Design Tool**: [Microsoft Teams App Icon Guidelines](https://learn.microsoft.com/en-us/microsoftteams/platform/concepts/build-and-test/apps-package#app-icons)
- **Icon Templates**: [Microsoft Teams Design Kit](https://learn.microsoft.com/en-us/microsoftteams/platform/concepts/design/design-teams-app-overview)
- **Free Icons**: [Flaticon](https://www.flaticon.com/) or [Icons8](https://icons8.com/) - search for "robot", "chatbot", or "assistant"
- **Online Editors**: [Canva](https://www.canva.com/) or [Figma](https://www.figma.com/) for customization

**Quick Icon Creation:**
1. Find or create a robot/chatbot icon
2. For `color.png`: Create 192x192px version with your brand colors
3. For `outline.png`: Create 32x32px white version on transparent background
4. Export as PNG files with these exact names

### 2.3 Package the App
1. Create a folder with these three files:
   - `manifest.json` (from step 2.1)
   - `color.png` (192x192 px)
   - `outline.png` (32x32 px)
2. Select all three files and create a ZIP archive
3. Name it `DatabricksGenieBot.zip`
4. **Important**: The files must be at the root of the ZIP, not in a subfolder

**Validation**: Use the [Teams App Validator](https://dev.teams.microsoft.com/appvalidation.html) to check your package before uploading

## Step 3: Deploy to Teams

### 3.1 Upload to Teams (Admin)
1. Go to [Microsoft Teams Admin Center](https://admin.teams.microsoft.com)
2. Navigate to **Teams apps** → **Manage apps**
3. Click **Upload** and select your `DatabricksGenieBot.zip`
4. Review and approve the app

### 3.2 Install for Users
1. In Teams, go to **Apps** → **Built for your org**
2. Find "Databricks Genie Bot"
3. Click **Add** to install

## Step 4: Test the Integration

### 4.1 Basic Functionality Test
1. Open a chat with the bot
2. Send a message: "help"
3. Verify you receive the help message
4. Test user identification: "whoami"

### 4.2 Databricks Integration Test
1. Ask a data-related question
2. Verify the bot processes the request
3. Check that responses are formatted correctly

## Step 5: Troubleshooting

### Common Issues

#### Bot Not Responding
- Check Azure App Service logs
- Verify messaging endpoint URL
- Ensure bot is running and healthy

#### User Identification Issues
- Verify Teams user has email configured
- Check bot's user session management
- Review authentication settings

#### Databricks API Errors
- Verify Databricks credentials
- Check space ID and permissions
- Review API token validity

### Debug Commands
- `/setuser email@company.com Name` (in emulator only)
- `whoami` - Check user session
- `help` - Show available commands

## Step 6: Production Considerations

### Security
- Use Azure Key Vault for secrets
- Implement proper authentication
- Regular security audits

### Monitoring
- Set up Application Insights
- Monitor bot performance
- Track user engagement

### Scaling
- Configure auto-scaling
- Monitor resource usage
- Plan for high availability

## Support
For issues specific to:
- **Bot Framework**: [Microsoft Bot Framework Documentation](https://docs.microsoft.com/en-us/azure/bot-service/)
- **Teams Integration**: [Microsoft Teams Bot Development](https://docs.microsoft.com/en-us/microsoftteams/platform/bots/what-are-bots)
- **Databricks API**: [Databricks Genie API Documentation](https://docs.databricks.com/)

## Next Steps
1. Complete the Azure Bot Service configuration
2. Create proper app icons
3. Package and deploy to Teams
4. Test thoroughly in your environment
5. Plan for user training and adoption
