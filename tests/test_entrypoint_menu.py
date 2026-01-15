import subprocess


def test_python_main_shows_menu():
    # Run the script with no input, it should print the interactive menu and exit on EOF
    p = subprocess.run(["python", "main.py"], input=b"", timeout=5, capture_output=True)
    out = p.stdout.decode("utf-8", errors="replace")
    assert "PGDataHub — interactive console" in out
    assert "Select an action" in out