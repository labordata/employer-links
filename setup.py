from setuptools import setup

setup(
    name="establishment",
    version="0.0.1",
    install_requires=["dedupe"],
    packages=["establishment"],
    package_data={"establishment": ["*.db", "*.json", "learned_settings"]}
)
