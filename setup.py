from setuptools import setup

# Metadata goes in setup.cfg. These are here for GitHub's dependency graph.
setup(
    name="kafkaopmon",
    install_requires=[
        "os",
		"re",
		"socket",
		"threading",
		"logging",
		"getpass",
		"sys",
		"inspect",
		"datetime",
		"time",
		"typing",
        "googleapis-common-protos",
        "kafka-python",
        "rich",
        "sh"

    ],
    extras_require={"develop": [
        "ipdb",
        "ipython"
    ]}
)