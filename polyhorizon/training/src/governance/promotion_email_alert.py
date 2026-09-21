from prefect import task
from prefect_email import EmailServerCredentials, email_send_message

from polyhorizon.training.configs.settings import Config
from polyhorizon.training.utils.logger import get_logger
logger = get_logger("Promotion Alert")


@task(name="Send Promotion Alert")
def send_promotion_alert(model_metadata: dict, 
                         metrics: dict, 
                         config: Config):
    """
    Sends a formatted HTML 'Battle Report' to the quant team.
    """
    # 1. Load your credentials from the Prefect UI/Secret store
    # Create this block in Prefect UI first (e.g., named 'trading-team-email')
    email_credentials_block = EmailServerCredentials.load("trading-team-email")
    
    subject = f"PROMOTED: {config.model_registry.name} v{model_metadata['model_version']}"
    
    # 2. Build a high-signal HTML "Battle Report"
    stability_line = ""
    if "stability" in metrics and metrics["stability"] is not None:
        stability_line = f"<li><b>Challenger Stability (Std):</b> {metrics['stability']:.6f}</li>"

    html_content = f"""
    <h3>Model Promotion Successful</h3>
    <p>The Challenger model has outperformed the current Champion and is now live.</p>
    <hr/>
    <table>
        <tr><td><b>Model Name:</b></td><td>{config.model_registry.name}</td></tr>
        <tr><td><b>New Version:</b></td><td>v{model_metadata['model_version']}</td></tr>
        <tr><td><b>Alias Assigned:</b></td><td>@{config.model_registry.champion_alias}</td></tr>
    </table>
    <h4>Financial Performance (Validation Set)</h4>
    <ul>
        <li><b>Challenger Hit Rate:</b> {metrics['challenger_hit_rate']:.2%}</li>
        <li><b>Challenger MAE:</b> {metrics['challenger_mae']:.6f}</li>
        {stability_line}
        <li><b>Improvement over Champ:</b> {metrics['improvement_pct']:.2%}</li>
    </ul>
    <p><a href="{config.mlflow.tracking_uri}/#/models/{config.model_registry.name}/versions/{model_metadata['model_version']}">
       View Model in MLflow
    </a></p>
    """

    email_send_message(
        email_server_credentials=email_credentials_block,
        subject=subject,
        msg_plain="A new model was promoted. Please view in HTML.",
        msg_html=html_content,
        email_to=config.logging.alert_emails 
    )
