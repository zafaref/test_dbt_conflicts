from prefect.blocks.notifications import SlackWebhook
from prefect.runtime import flow_run
from prefect import get_run_logger


def notify_slack(slack_webhook_block, subject, message):
    logger = get_run_logger()
    try:
        message += f"\n<{flow_run.ui_url}|Flow run URL>"
        slack = SlackWebhook.load(slack_webhook_block, _sync=True)
        slack.notify(message, subject, _sync=True)
        logger.info(f"Sent Slack message to {slack_webhook_block}")
    except Exception as e:
        logger.warning(f"Failed to send Slack alert: {e}")
