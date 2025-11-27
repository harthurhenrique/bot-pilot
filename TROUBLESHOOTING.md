# Troubleshooting Guide

## Common Issues and Solutions

### 1. Databricks Token Not Found Error

**Error Message:**
```
ValueError: default auth: cannot configure default credentials, please check https://docs.databricks.com/en/dev-tools/auth.html#databricks-client-unified-authentication to configure credentials for your preferred authentication method.
```

**Cause:** The `DATABRICKS_TOKEN` environment variable is not being loaded properly in Azure.

**Solutions:**

#### Option A: Azure App Service Configuration
1. Go to Azure Portal → Your App Service
2. Navigate to **Configuration** → **Application settings**
3. Add the following environment variables:
   - `DATABRICKS_TOKEN`: Your Databricks personal access token
   - `DATABRICKS_HOST`: Your Databricks workspace URL
   - `DATABRICKS_SPACE_ID`: Your Databricks space ID

#### Option B: Check Environment Variable Names
Ensure the environment variable names match exactly (case-sensitive):
- `DATABRICKS_TOKEN` (not `databricks_token` or `Databricks_Token`)
- `DATABRICKS_HOST` (not `databricks_host`)

#### Option C: Verify Token Format
The token should start with `dapi` and be the full personal access token:
```
DATABRICKS_TOKEN=dapi1234567890abcdef1234567890ab
```
Note: Do not include quotes around the token value in environment variables.

#### Option D: Test Locally First
1. Create a `.env` file with your variables:
   ```bash
   DATABRICKS_TOKEN=your-token-here
   DATABRICKS_HOST=https://your-workspace.cloud.databricks.com/
   DATABRICKS_SPACE_ID=your-space-id
   ```
2. Test locally to ensure it works
3. Deploy to Azure with the same values

### 2. Bot Framework Authentication Issues

**Error Message:**
```
BotFrameworkAdapter: Authentication failed
```

**Solutions:**
1. Verify `APP_ID` and `APP_PASSWORD` are set in Azure App Service
2. Check that the messaging endpoint is correct
3. Ensure the bot is registered in Azure Bot Service

### 3. Teams Integration Issues

**Error Message:**
```
Bot not responding in Teams
```

**Solutions:**
1. Verify Teams channel is enabled in Azure Bot Service
2. Check that the bot is installed in Teams
3. Review the Teams app manifest configuration

### 4. User Email Identification Issues

**Problem:** Bot keeps asking for email even after providing it

**Solutions:**
1. Verify email format is valid (e.g., `user@company.com`)
2. Check that you didn't type `cancel` accidentally
3. If using Bot Emulator, use `/setuser email@company.com Your Name` command
4. Clear your session with `logout` and try again

**Problem:** Bot doesn't recognize my email

**Solutions:**
1. Ensure email follows standard format: `name@domain.com`
2. Check for extra spaces before or after the email
3. Try the `whoami` command to verify your session
4. If in Teams, check that your Teams profile has an email configured

### 5. Genie Space Configuration Issues

**Error Message:**
```
Invalid Space ID or access denied
```

**Solutions:**
1. Verify `DATABRICKS_SPACE_ID` is correct
2. Ensure your Databricks token has access to the specified Genie Space
3. Check that the Genie Space exists and is active
4. Verify the Space ID format (should be a long alphanumeric string)

**Error Message:**
```
Genie API returned no results
```

**Solutions:**
1. The Space might be empty or have no data
2. Verify the Space has been properly configured in Databricks
3. Test the Space directly in Databricks UI first
4. Check that your query is compatible with the data in the Space

**Error Message (User sees):**
```
No data available.
```

**Error Message (In Logs):**
```
403 Forbidden
Source IP address: X.X.X.X is blocked by Databricks IP ACL for workspace
```

**Cause:** Your Azure App Service's outbound IP address is blocked by Databricks Account IP Access Lists (ACLs)

**Solutions:**

1. **Find Your Azure App Service Outbound IPs:**
   - Go to Azure Portal → Your App Service
   - Navigate to **Networking** (in the left sidebar)
   - Look for **Outbound IP addresses** section
   - Copy all the IP addresses listed (there may be several)
   - Example: `198.51.100.10, 198.51.100.20, 198.51.100.30`

2. **Add IPs to Databricks Account Allow List (Web UI Method):**
   - Log into your **Databricks Account Console** (not workspace)
   - In the sidebar, click **Settings**
   - Navigate to **Security and Compliance** tab
   - Click **IP Access List**
   - Click **Add rule**
   - Add each Azure App Service outbound IP address
   - Label it clearly (e.g., "teams-genie-bot")
   - Select **ALLOW** as the list type
   - Click **Add rule** to save

   **For detailed instructions**, see: [Configure IP access lists for the account console](https://docs.databricks.com/aws/en/security/network/front-end/ip-access-list-account)

3. **Add IPs to Databricks Account Allow List (CLI Method):**
   
   If you prefer using the Databricks CLI, you can add IPs programmatically to your Account:

   ```bash
   databricks ip-access-lists create --json '{
     "label": "teams-genie-bot",
     "list_type": "ALLOW",
     "ip_addresses": [
       "198.51.100.10/32",
       "198.51.100.20/32",
       "198.51.100.30/32"
     ]
   }'
   ```

   Replace the IP addresses with your actual Azure App Service outbound IPs. The `/32` suffix means a single IP address.

   **Note**: Make sure your CLI is configured to use account-level authentication.

   **For CLI documentation**, see: [Databricks CLI IP Access Lists Commands](https://docs.databricks.com/aws/en/dev-tools/cli/reference/ip-access-lists-commands)

4. **Alternative: Use Azure VNet Integration** (Advanced):
   - Configure Azure App Service VNet integration
   - Set up private endpoint to Databricks
   - This provides a more secure, stable IP address

5. **Test the Connection:**
   - After adding IPs to the Account allow list, wait a few minutes for changes to take effect
   - Try querying the bot again
   - Check Azure App Service logs to confirm the 403 error is gone
   - If still blocked, verify the IP in the error log matches the IPs you added

**Important Notes:**
- **Account vs Workspace**: This error is at the **Account level**, not workspace level. Make sure you're adding IPs in the Account Console, not workspace settings
- **IP Changes**: Azure App Service outbound IPs can change during scaling or updates
- **Production Recommendation**: Consider using a static outbound IP with Azure NAT Gateway for production deployments
- **Documentation**: Document all IPs added to Databricks Account ACL for future reference
- **Maximum IPs**: Databricks supports a maximum of 1000 IP/CIDR values across all allow and block lists
- **Verification**: Always verify the IP in the error log matches the IPs you added to the allow list

### 6. Feedback System Issues

**Problem:** Feedback buttons (👍/👎) not appearing

**Solutions:**
1. Check that `ENABLE_FEEDBACK_CARDS=True` in environment variables
2. Verify you're using a client that supports Adaptive Cards (Teams does)
3. Check Azure App Service logs for card rendering errors

**Problem:** Feedback submission fails

**Solutions:**
1. Verify `ENABLE_GENIE_FEEDBACK_API=True` in environment variables
2. Check that your Databricks token has permissions to send feedback
3. Review logs for specific API error messages
4. Ensure the Genie message ID is valid

### 7. Conversation and Session Issues

**Problem:** Conversation resets unexpectedly

**Cause:** Conversations automatically reset after 4 hours of inactivity

**Solutions:**
1. This is expected behavior - just start a new question
2. Use `reset` or `new chat` command to manually start fresh
3. The 4-hour timeout is configurable in the code if needed

**Problem:** Bot doesn't remember previous conversation

**Solutions:**
1. Check if more than 4 hours have passed (automatic timeout)
2. Verify you're using the same user account
3. Try `whoami` to check your session status
4. Use `reset` to start a fresh conversation if needed

### 8. Sample Questions Not Showing

**Problem:** Sample questions are not displaying or are generic

**Solutions:**
1. Set `SAMPLE_QUESTIONS` environment variable with custom questions
2. Format: `Question 1;Question 2;Question 3` (semicolon-delimited)
3. Restart the App Service after changing the configuration
4. Check for syntax errors in your question list (no stray semicolons)

**Example Configuration:**
```bash
SAMPLE_QUESTIONS=What are my top products?;Show me sales trends;Who are my best customers?
```

### 9. Debugging Steps

#### Check Logs
1. Go to Azure Portal → Your App Service
2. Navigate to **Monitoring** → **Log stream**
3. Look for the debug messages we added:
   ```
   Loading Databricks configuration...
   DATABRICKS_HOST: https://your-workspace.cloud.databricks.com/
   DATABRICKS_TOKEN present: True
   DATABRICKS_TOKEN length: 40
   ```

#### Test Environment Variables
Add this to your app.py temporarily to debug:
```python
import os
print("All environment variables:")
for key, value in os.environ.items():
    if 'DATABRICKS' in key or 'APP_' in key:
        print(f"{key}: {value[:10]}..." if len(value) > 10 else f"{key}: {value}")
```

### 10. Admin Contact Email Not Showing

**Problem:** Info command doesn't show admin contact email or shows default

**Solutions:**
1. Set `ADMIN_CONTACT_EMAIL` in environment variables
2. Format: `ADMIN_CONTACT_EMAIL=support@yourcompany.com`
3. Restart the App Service after configuration
4. Test with the `info` command

### 11. Performance and Timeout Issues

**Problem:** Bot responses are very slow

**Solutions:**
1. Check Databricks API performance and quota limits
2. Verify network connectivity between Azure and Databricks
3. Review Azure App Service plan (consider upgrading for better performance)
4. Check if Genie Space has large datasets causing slow queries

**Problem:** Bot times out without responding

**Solutions:**
1. Increase Azure App Service timeout settings
2. Optimize Genie Space queries for better performance
3. Check Azure App Service logs for timeout errors
4. Verify Databricks workspace is responding

### 12. Bot Commands Not Working

**Problem:** Commands like `help`, `info`, `whoami` not responding

**Solutions:**
1. Commands are case-insensitive but check for typos
2. Ensure you're logged in (provide email first if needed)
3. Try with `/` prefix: `/help`, `/info`, `/whoami`
4. Check if bot is responding at all (try a simple message)

**Available Commands:**
- `help` or `/help` - Show all commands
- `info` or `/info` - Show bot information
- `whoami` or `/whoami` - Display user info
- `reset` or `new chat` - Start fresh conversation
- `logout` - Clear session
- `email` - Provide email address (when prompted)

### 13. Common Configuration Mistakes

1. **Missing Environment Variables**: Ensure all required variables are set
2. **Wrong Variable Names**: Check for typos in variable names (case-sensitive)
3. **Token Format**: Ensure token is complete and valid (starts with `dapi`)
4. **Azure Configuration**: Verify App Service settings are saved
5. **Restart Required**: Always restart the App Service after changing configuration
6. **Quotes in Environment Variables**: Don't use quotes around values in Azure App Service settings
7. **Semicolon Delimiter**: For `SAMPLE_QUESTIONS`, use semicolons `;` not commas or other delimiters

### 14. Getting Help

If you're still having issues:

1. **Check Logs First**: Azure App Service → Monitoring → Log stream
2. **Test Locally**: Use Bot Framework Emulator to test before deploying
3. **Verify Configuration**: Double-check all environment variables
4. **Contact Admin**: Use the `info` command to get admin contact email
5. **Review Documentation**: 
   - [Databricks Genie API Docs](https://docs.databricks.com/)
   - [Bot Framework Docs](https://docs.microsoft.com/en-us/azure/bot-service/)
   - [Teams Bot Development](https://docs.microsoft.com/en-us/microsoftteams/platform/bots/what-are-bots)

### 15. Environment Variables Quick Reference

**Required:**
- `DATABRICKS_TOKEN` - Your Databricks personal access token
- `APP_ID` - Azure Bot Service Application ID (production)
- `APP_PASSWORD` - Azure Bot Service Application Password (production)

**Recommended:**
- `DATABRICKS_SPACE_ID` - Your Genie Space ID
- `DATABRICKS_HOST` - Your Databricks workspace URL
- `SAMPLE_QUESTIONS` - Custom sample questions (semicolon-delimited)
- `ADMIN_CONTACT_EMAIL` - Support contact email

**Optional:**
- `PORT` - Application port (default: 3978)
- `APP_TYPE` - SingleTenant or MultiTenant (default: SingleTenant)
- `APP_TENANTID` - Azure Tenant ID
- `ENABLE_FEEDBACK_CARDS` - Enable feedback UI (default: True)
- `ENABLE_GENIE_FEEDBACK_API` - Send feedback to Genie (default: True)

### 16. Quick Fix Checklist

**Environment Setup:**
- [ ] `DATABRICKS_TOKEN` is set in Azure App Service
- [ ] `DATABRICKS_HOST` is set correctly (include https://)
- [ ] `DATABRICKS_SPACE_ID` is set
- [ ] Token has access to the specified Genie Space

**Bot Configuration:**
- [ ] `APP_ID` is set (for production)
- [ ] `APP_PASSWORD` is set (for production)
- [ ] Bot Framework credentials are configured in Azure Bot Service
- [ ] Messaging endpoint is correct: `https://your-app.azurewebsites.net/api/messages`

**Optional Features:**
- [ ] `SAMPLE_QUESTIONS` is customized for your use case
- [ ] `ADMIN_CONTACT_EMAIL` is set to your support email
- [ ] Feedback settings are configured as desired

**Deployment:**
- [ ] App Service has been restarted after configuration changes
- [ ] Logs show successful Databricks client initialization
- [ ] Teams channel is enabled in Azure Bot Service
- [ ] Bot is installed and accessible in Teams

**Testing:**
- [ ] Test `help` command works
- [ ] Test `info` command shows correct information
- [ ] Test data query returns results
- [ ] Test feedback buttons appear (if enabled)
- [ ] Test `whoami` shows correct user info
