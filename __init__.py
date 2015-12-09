"""
filesystem provides a set of storage abstractions for applications with
more complicated needs than what can be satisfied by the base file system.

Storage Abstractions:

	/hash
		Hash based file addressing for complex storage keys.

	/flow
		Event stream segmentation for managing the archival process
		of a window's past events. (Similar to log file rotation)
"""
__pkg_bottom__ = True # Use this to detect the root package module of a project.
__type__ = 'project'
