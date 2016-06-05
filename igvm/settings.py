COMMON_FABRIC_SETTINGS = dict(
    disable_known_hosts=True,
    use_ssh_config=True,
    always_use_pty=False,
    forward_agent=True,
    user='root',
    shell='/bin/bash -c',
)
