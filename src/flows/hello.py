"""
Hello World Flow - Test Prefect Setup
======================================
A simple flow to verify Prefect is working correctly.
"""

from prefect import flow, task, get_run_logger


@task
def say_hello(name: str) -> str:
    """A simple task that says hello."""
    logger = get_run_logger()
    message = f"Hello, {name}!"
    logger.info(message)
    return message


@task
def count_letters(message: str) -> int:
    """Count letters in the message."""
    logger = get_run_logger()
    count = len(message)
    logger.info(f"Message has {count} characters")
    return count


@flow(name="hello-world", log_prints=True)
def hello_flow(name: str = "NEDL") -> dict:
    """
    A simple hello world flow to test Prefect setup.
    
    Args:
        name: Name to greet
        
    Returns:
        Dict with greeting and character count
    """
    logger = get_run_logger()
    logger.info(f"🚀 Starting hello flow for {name}")
    
    # Run tasks
    message = say_hello(name)
    count = count_letters(message)
    
    logger.info("✅ Flow complete!")
    
    return {
        "message": message,
        "character_count": count
    }


if __name__ == "__main__":
    # Run locally for testing
    result = hello_flow(name="Wilson")
    print(result)

