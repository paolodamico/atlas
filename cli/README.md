# atlas-cli

A command-line client over `atlas-core`, and the vehicle for the offline-sync
demo. Each vault is a directory (`--vault <dir>`, default `atlas-vault`).

## Notes

- Notes are addressed by id, a unique id prefix (as shown by `list`), or exact
  path.
- `merge <dir>` reconciles two local vault directories by merging whole docs.
- `sync --relay <url> --graph <name>` exchanges incremental changes with a
  relay: it pushes local changes and pulls remote ones. Run it from each
  device sharing a graph to keep them converged.

## Walkthrough: concurrent offline edits merge through a relay

Start a relay (defaults to `127.0.0.1:4000`):

```sh
cargo run -p relay
```

Then, with `URL=http://127.0.0.1:4000`:

```sh
atlas --vault ./alpha init
atlas --vault ./beta init

atlas --vault ./alpha add n.md --title Note --body "one two three"
atlas --vault ./alpha sync --relay $URL --graph demo   # alpha pushes
atlas --vault ./beta  sync --relay $URL --graph demo   # beta pulls the note

# Both devices edit the same note while "offline":
atlas --vault ./alpha edit n.md --body "ONE two three"
atlas --vault ./beta  edit n.md --body "one two THREE"

atlas --vault ./alpha sync --relay $URL --graph demo
atlas --vault ./beta  sync --relay $URL --graph demo
atlas --vault ./alpha sync --relay $URL --graph demo

atlas --vault ./alpha show n.md    # ONE two THREE
atlas --vault ./beta  show n.md    # ONE two THREE
```

Without a relay, `merge` does the same reconciliation between two local dirs:

```sh
atlas --vault ./beta merge ./alpha
```
