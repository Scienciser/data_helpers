"""
Read first n bytes or lines of text file that would otherwise choke any regular text editor.
"""
def readBigFile(path, n, mode='lines'):
    with open(path, 'r', encoding='utf8') as myFile:
        if mode == 'bytes':
            x = myFile.read(n)
        elif mode == 'lines':
            x = "".join([next(myFile) for i in range(n)])
        else:
            raise Exception('Invalid mode')
    return x


"""
Upgrade all pip packages
"""
def updateAllPackages():
    import pkg_resources
    from subprocess import call

    packages = [dist.project_name for dist in pkg_resources.working_set]
    call("pip install --upgrade " + ' '.join(packages), shell=True)
