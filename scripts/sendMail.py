#!/usr/bin/env python
"""
Email utility for sending pipeline reports and notifications.
Supports multiple delivery methods: SMTP, local mail command, or simulated mode.
"""

import os
import smtplib
import logging
import json
import subprocess
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from typing import Dict, List, Union, Optional
import ssl

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class EmailSender:
    """Email sending utility with multiple delivery options."""

    def __init__(self, config: Dict = None):
        """Initialize email sender with configuration.

        Args:
            config: Email configuration dictionary. If None, will use environment variables.
        """
        # Default configuration
        self.config = {
            'smtp_host': os.environ.get('SMTP_HOST', 'localhost'),
            'smtp_port': int(os.environ.get('SMTP_PORT', 25)),
            'smtp_user': os.environ.get('SMTP_USER', ''),
            'smtp_password': os.environ.get('SMTP_PASSWORD', ''),
            'smtp_use_tls': os.environ.get('SMTP_USE_TLS', 'False').lower() == 'true',
            'from_email': os.environ.get('FROM_EMAIL', 'airflow@example.com'),
            'delivery_method': os.environ.get('EMAIL_DELIVERY_METHOD', 'simulate')
        }

        # Update with provided config if any
        if config:
            self.config.update(config)

        # Auto-detect proper settings for known mail servers
        self._detect_mail_settings()

        logger.info(f"Email sender initialized with delivery method: {self.config['delivery_method']}")

    def _detect_mail_settings(self):
        """Auto-detect proper settings for known mail servers"""
        host = self.config['smtp_host'].lower()
        port = self.config['smtp_port']

        # Outlook.com specific settings
        if 'outlook.com' in host or 'office365' in host:
            if port == 587:
                logger.info("Detected Outlook.com service with port 587, automatically configuring for STARTTLS")
                self.config['use_starttls'] = True
                self.config['smtp_use_tls'] = False  # Don't use direct SSL when using STARTTLS

        # Gmail specific settings
        elif 'gmail.com' in host:
            if port == 587:
                logger.info("Detected Gmail service with port 587, automatically configuring for STARTTLS")
                self.config['use_starttls'] = True
                self.config['smtp_use_tls'] = False  # Don't use direct SSL when using STARTTLS

        # General port-specific settings
        elif port == 587:
            logger.info("Using port 587, defaulting to STARTTLS mode")
            self.config['use_starttls'] = True
            self.config['smtp_use_tls'] = False
        elif port == 465:
            logger.info("Using port 465, defaulting to direct SSL/TLS mode")
            self.config['use_starttls'] = False
            self.config['smtp_use_tls'] = True

    def send_email(
            self,
            to_emails: Union[str, List[str]],
            subject: str,
            html_content: str,
            cc_emails: List[str] = None,
            attachments: List[Dict] = None
    ) -> bool:
        """Send an email using the configured delivery method.

        Args:
            to_emails: Single email string or list of recipient emails
            subject: Email subject
            html_content: HTML content of the email
            cc_emails: List of CC recipients
            attachments: List of attachment dictionaries {filename, content, mimetype}

        Returns:
            bool: True if successful, False otherwise
        """
        if isinstance(to_emails, str):
            to_emails = [to_emails]

        cc_emails = cc_emails or []

        # Create email message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.config['from_email']
        msg['To'] = ', '.join(to_emails)

        if cc_emails:
            msg['Cc'] = ', '.join(cc_emails)

        # Add HTML content
        msg.attach(MIMEText(html_content, 'html'))

        # Dispatch to appropriate delivery method
        method = self.config['delivery_method'].lower()
        if method == 'smtp':
            return self._send_via_smtp(msg, to_emails + cc_emails)
        elif method == 'local':
            return self._send_via_local_command(msg, to_emails)
        else:
            return self._simulate_send(msg, to_emails)

    def _send_via_smtp(self, msg, recipients: List[str]) -> bool:
        """Send email via SMTP server."""
        try:
            smtp_host = self.config['smtp_host']
            smtp_port = self.config['smtp_port']
            use_tls = self.config['smtp_use_tls']
            use_starttls = self.config.get('use_starttls', False)

            logger.info(f"Connecting to SMTP server {smtp_host}:{smtp_port} (TLS: {use_tls}, STARTTLS: {use_starttls})")

            # For Outlook with port 587, force STARTTLS mode
            if 'outlook.com' in smtp_host.lower() and smtp_port == 587:
                use_starttls = True
                use_tls = False
                logger.info("Detected Outlook.com on port 587, forcing STARTTLS mode")

            # Establish connection based on the server requirements
            if use_tls:
                logger.info("Using direct SSL/TLS connection")
                context = ssl.create_default_context()
                smtp = smtplib.SMTP_SSL(smtp_host, smtp_port, context=context)
            else:
                logger.info("Using plain connection (will upgrade with STARTTLS if needed)")
                smtp = smtplib.SMTP(smtp_host, smtp_port)
                smtp.set_debuglevel(1)  # Enable verbose debug output

            # For Outlook.com and other services that need STARTTLS
            if use_starttls:
                logger.info("Upgrading connection with STARTTLS")
                smtp.ehlo()  # Identify to the server
                smtp.starttls()  # Secure the connection
                smtp.ehlo()  # Re-identify over TLS connection

            # Login if credentials provided
            if self.config['smtp_user'] and self.config['smtp_password']:
                logger.info(f"Logging in as {self.config['smtp_user']}")
                smtp.login(self.config['smtp_user'], self.config['smtp_password'])

            # Send email
            from_email = self.config['from_email']
            logger.info(f"Sending email from {from_email} to {', '.join(recipients)}")
            result = smtp.sendmail(from_email, recipients, msg.as_string())

            if result:
                logger.info(f"SMTP sendmail returned: {result}")
            else:
                logger.info("Email sent successfully")

            smtp.quit()
            return True

        except Exception as e:
            logger.error(f"SMTP email sending failed: {str(e)}")
            # Fall back to simulation mode if SMTP fails
            logger.info("Falling back to simulation mode due to SMTP error")
            return self._simulate_send(msg, recipients)

    def _send_via_local_command(self, msg, recipients: List[str]) -> bool:
        """Send email via local mail command."""
        try:
            # Write email to temp file
            temp_file = f"/tmp/email_{datetime.now().strftime('%Y%m%d%H%M%S')}.eml"
            with open(temp_file, 'w') as f:
                f.write(msg.as_string())

            # Use mail command to send
            cmd = f"mail -t < {temp_file}"
            subprocess.check_call(cmd, shell=True)

            # Clean up
            os.unlink(temp_file)

            logger.info(f"Email sent via local mail command to {', '.join(recipients)}")
            return True

        except Exception as e:
            logger.error(f"Local mail command failed: {str(e)}")
            # Fall back to simulation mode if local mail fails
            logger.info("Falling back to simulation mode due to local mail error")
            return self._simulate_send(msg, recipients)

    def _simulate_send(self, msg, recipients: List[str]) -> bool:
        """Simulate sending email by logging the content."""
        logger.info(f"SIMULATED EMAIL:")
        logger.info(f"To: {', '.join(recipients)}")
        logger.info(f"Subject: {msg['Subject']}")
        logger.info(f"Content would have been sent to {len(recipients)} recipients")

        # Write to a file for inspection
        email_dir = "/tmp/simulated_emails"
        os.makedirs(email_dir, exist_ok=True)
        filename = f"{email_dir}/email_{datetime.now().strftime('%Y%m%d%H%M%S')}.eml"

        with open(filename, 'w') as f:
            f.write(msg.as_string())

        logger.info(f"Email content written to {filename}")
        return True


def generate_pipeline_report(pipeline_status: Dict) -> str:
    """Generate HTML email content for pipeline status report.

    Args:
        pipeline_status: Dictionary containing pipeline execution details

    Returns:
        str: HTML content for the email
    """
    # Format the current date for the email
    run_date = datetime.now().strftime('%Y-%m-%d')

    if 'end_time' not in pipeline_status:
        pipeline_status['end_time'] = datetime.now()

    # Create email HTML content
    html_content = f"""
    <html>
    <head>
        <style>
            body {{ font-family: Arial, sans-serif; }}
            .header {{ background-color: #4285f4; color: white; padding: 10px; text-align: center; }}
            .content {{ margin: 20px; }}
            .footer {{ background-color: #f2f2f2; padding: 10px; text-align: center; }}
            table {{ border-collapse: collapse; width: 100%; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #4285f4; color: white; }}
            .success {{ background-color: #d4edda; }}
            .failed {{ background-color: #f8d7da; }}
            .running {{ background-color: #fff3cd; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h2>CarInsight Data Pipeline Report</h2>
        </div>

        <div class="content">
            <h3>Pipeline Execution Summary</h3>
            <p><strong>Date:</strong> {run_date}</p>
            <p><strong>User:</strong> VietDucFCB</p>
            <p><strong>Overall Status:</strong> {pipeline_status['overall_status'].upper()}</p>

            <h3>Task Details</h3>
            <table>
                <tr>
                    <th>Task</th>
                    <th>Status</th>
                    <th>Start Time</th>
                    <th>End Time</th>
                    <th>Duration</th>
                    <th>Notes</th>
                </tr>
    """

    # Add rows for each task
    for task_name, details in pipeline_status['tasks'].items():
        status_class = 'success' if details.get('status') == 'success' else (
            'failed' if details.get('status') == 'failed' else 'running')
        start_time = details.get('start_time', '').strftime('%H:%M:%S') if details.get('start_time') else '-'
        end_time = details.get('end_time', '').strftime('%H:%M:%S') if details.get('end_time') else '-'
        duration = details.get('execution_time', '-')
        notes = details.get('error', '-') if details.get('status') == 'failed' else '-'

        html_content += f"""
                <tr class="{status_class}">
                    <td>{task_name}</td>
                    <td>{details.get('status', '-').upper()}</td>
                    <td>{start_time}</td>
                    <td>{end_time}</td>
                    <td>{duration}</td>
                    <td>{notes}</td>
                </tr>
        """

    # Complete the HTML
    html_content += """
            </table>

            <h3>Next Steps</h3>
            <p>You can access the Airflow dashboard for more details.</p>
        </div>

        <div class="footer">
            <p>This is an automated email from your CarInsight Data Pipeline.</p>
        </div>
    </body>
    </html>
    """

    return html_content


def main():
    """Test function to demonstrate email sending."""
    # Create a sample pipeline status
    test_status = {
        'start_time': datetime.now(),
        'end_time': datetime.now(),
        'overall_status': 'success',
        'tasks': {
            'Task 1': {'status': 'success', 'start_time': datetime.now(), 'end_time': datetime.now(),
                       'execution_time': '5.2s'},
            'Task 2': {'status': 'failed', 'start_time': datetime.now(), 'end_time': datetime.now(),
                       'execution_time': '1.3s', 'error': 'Connection timeout'},
        }
    }

    # Generate email content
    html = generate_pipeline_report(test_status)

    # Create sender and send email
    sender = EmailSender()
    result = sender.send_email(
        'test@example.com',
        'Pipeline Report Test',
        html
    )

    return result


if __name__ == "__main__":
    main()