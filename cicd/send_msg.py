import argparse
import requests
import os


def run():
    # Set up the argument parser
    parser = argparse.ArgumentParser(description="Send a message to a Telegram chat")
    parser.add_argument(
        "--message", type=str, required=True, help="The message to send to Telegram"
    )

    # Parse the command-line arguments
    args = parser.parse_args()

    # Replace these with your actual bot token and chat ID
    MESSAGE_THREAD_ID = os.environ.get("CICD_MESSAGE_THREAD_ID", "...")
    BOT_TOKEN = os.environ.get("BOT_TOKEN", "...")
    CHAT_ID = os.environ.get("CICD_CHAT_ID", "...")
    MESSAGE = args.message

    # Create the payload as a dictionary
    payload = {
        "message_thread_id": MESSAGE_THREAD_ID,
        "chat_id": CHAT_ID,
        "text": MESSAGE,
    }

    # Define the URL for the Telegram Bot API
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

    # Send the POST request to Telegram's API
    response = requests.post(url, json=payload)

    # Print the response from Telegram (for debugging purposes)
    print(response.text)


if __name__ == "__main__":
    run()
