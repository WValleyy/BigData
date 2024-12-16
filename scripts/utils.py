import os
from dotenv import load_dotenv

def load_environment_variables(env_file_path=".env"):
    """
    Loads environment variables from a .env file and returns them as a dictionary.

    Args:
        env_file_path (str): Path to the .env file (default is current directory).

    Returns:
        dict: A dictionary containing all environment variables from the file.
    """
    try:
        # Load the .env file
        load_dotenv(env_file_path)

        # Retrieve all environment variables
        env_vars = {key: os.getenv(key) for key in os.environ}
        print(f"Successfully loaded environment variables from {env_file_path}")
        return env_vars
    except Exception as e:
        print(f"Error loading environment variables: {e}")
        return {}
