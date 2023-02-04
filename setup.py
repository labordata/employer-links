from setuptools import setup

setup(
    name="establishment",
    version="0.0.1",
    install_requires=["dedupe", "click"],
    packages=["establishment"],
    package_data={"establishment": ["*.db", "*.json", "learned_settings"]},
    entry_points={"console_scripts": ["employerlookup=establishment:main"]},
)
