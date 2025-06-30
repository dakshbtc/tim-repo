from flask import Flask
from flask import request
from config import *
import requests
from schwab import *

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def refresh_access_token():
    try:
        print(request)
        print(request.args)
        code = request.args.get("code")
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": api_callback_url,
        }

        logger = logging.getLogger(__name__)
        response = requests.post(
            authtoken_link, headers=create_header("Basic", logger), data=data
        )
        response = response.json()
        print(response)
        if "error" in response.keys():
            logger.error(
                f"Error in refreshing access token = {response['error_description']}"
            )
            return f"Error in refreshing access token = {response['error_description']}"
        with open(refresh_token_path, "w") as file:
            file.write(response["refresh_token"])

        with open(access_token_path, "w") as file:
            file.write(response["access_token"])
        print("Access token refreshed")
        logger.info(f"Access token refreshed at {datetime.now(tz=timezone(time_zone))}")
        return "Access token refreshed yeaayyyyyy"

    except Exception as e:
        print(f"Error in refreshing access token = {str(e)}")
        # sleep(10)
        # refresh_access_token()


if __name__ == "__main__":
    app.run(port=5000, debug=True)
