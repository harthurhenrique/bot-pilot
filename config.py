import os

class DefaultConfig:
    """Bot Configuration"""

    # Configuração da Porta
    PORT = int(os.getenv("PORT", "3978"))
    
    # Credenciais do Bot Framework - usa variáveis de ambiente com valores de fallback para testes locais
    APP_ID = os.getenv("APP_ID", "")  # Vazio para teste no emulador, definido para produção
    APP_PASSWORD = os.getenv("APP_PASSWORD", "")  # Vazio para teste no emulador, definido para produção
    APP_TYPE = os.getenv("APP_TYPE", "SingleTenant") 
    APP_TENANTID = os.getenv("APP_TENANTID", "")
    

    
    # Configuração do Databricks - usa variáveis de ambiente com valores de fallback
    DATABRICKS_SPACE_ID = os.getenv("DATABRICKS_SPACE_ID", "")
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
    
    # Valida variáveis de ambiente obrigatórias (pula validação para teste no emulador)
    if not DATABRICKS_TOKEN and APP_ID:  
        raise ValueError("DATABRICKS_TOKEN environment variable is required")
    
    # Configuração de perguntas de exemplo
    SAMPLE_QUESTIONS = os.getenv(
        "SAMPLE_QUESTIONS",
        "Quais dados estão disponíveis?;"  
        "Pode explicar o conjunto de dados?;"                      
        "Que perguntas posso fazer?"
    )
    
    # E-mail de contato do administrador
    #ADMIN_CONTACT_EMAIL = os.getenv("ADMIN_CONTACT_EMAIL", "admin@company.com")
    
    # Configurações de Feedback
    ENABLE_FEEDBACK_CARDS = os.getenv("ENABLE_FEEDBACK_CARDS", "True").lower() == "true"
    ENABLE_GENIE_FEEDBACK_API = os.getenv("ENABLE_GENIE_FEEDBACK_API", "True").lower() == "true"