"""
[ Structure ]

The structure of the project. Prevalent high-level concepts.
Where such concepts are manifested.

[ Requirements ]

Any hidden dependencies or notes about dependencies.

[ Defense ]

No general, isolated solution available.
"""
abstract = 'storage abstractions for event streams and sub-filesystems'

#: Project name.
name = 'filesystem'

#: The name of the conceptual branch of development.
fork = 'lattice' # Explicit branch name and a codename for the major version of the project.
release = None # A number indicating its position in the releases of a branch. (fork)

#: The particular study or subject that the package is related to.
study = {}

#: Relevant emoji or reference--URL or relative file path--to an image file.
icon = '💾'

#: IRI based project identity. (project homepage)
identity = 'https://fault.io/project/python/filesystem'

#: Responsible Party
controller = 'fault.io'

#: Contact point for the Responsible Party
contact = 'mailto:critical@fault.io'

#: Version tuple: (major, minor, patch)
version_info = (0, 1, 0)

#: The version string.
version = '.'.join(map(str, version_info))
