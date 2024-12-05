def printSkipped():
    RED = "\033[0;31m"
    WHITE = "\033[0m"
    print(f"[{RED}Skipped{WHITE}]  ", end="")

def printIncluded():
    GREEN = "\033[0;32m"
    WHITE = "\033[0m"
    print(f"[{GREEN}Included{WHITE}] ", end="")
