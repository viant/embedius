# list
List files under a configured root.

Inputs (JSON):
- rootId: root id from roots (required)
- path: optional subpath
- recursive: walk subtree when true
- include/exclude: glob patterns; supports **
- maxItems: cap returned items

Output:
- items: list of files/dirs with uri, path, name, size, modified, rootId
- total: number of items returned
