c = get_config()  # noqa: F821
c.ServerApp.ip = "0.0.0.0"
c.ServerApp.port = 8888
c.ServerApp.open_browser = False
c.ServerApp.root_dir = "/work"
# No token. This is a teaching container bound to localhost on the student's own
# machine; a token is one more thing that goes wrong at 09:02 in session 1.
c.ServerApp.token = ""
c.ServerApp.password = ""
c.ServerApp.allow_origin = "*"
c.ServerApp.disable_check_xsrf = True
c.ServerApp.terminado_settings = {"shell_command": ["/bin/bash"]}
