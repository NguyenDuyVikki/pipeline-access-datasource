from openai import Pipeline

def test_pipe():
    pipeline = Pipeline()

    # Example natural language query
    user_message = "Show me 5 user onboardings."

    # Call the pipe function
    result = pipeline.pipe(user_message, model_id="gpt-4-turbo", messages=[], body={})
    return result




if __name__ == "__main__":
    test_pipe()
