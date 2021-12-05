def get_version():
    with open('VERSION') as version_file:
        version = version_file.read()
    return version.strip()
