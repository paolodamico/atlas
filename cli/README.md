# atlas-cli

A command-line client over `atlas-core`, and the vehicle for the offline-sync
demo. Each vault is a directory (`--vault <dir>`, default `atlas-vault`).

## 📔 Notes

- Notes are addressed by id, a unique id prefix (as shown by `list`), or exact
  path.
- `merge <dir>` reconciles two local vault directories by merging whole docs.
- `sync --relay <url> --graph <name>` exchanges incremental changes with a
  relay: it pushes local changes and pulls remote ones. Run it from each
  device sharing a graph to keep them converged.
- `live <note> --relay <ws-url> --graph <name>` is a long-running editor: it
  renders remote changes live over a websocket and appends each line you type.

## 🦮 Walkthroughs

### Live two-terminal demo

Start a relay (`cargo run -p atlas-relay`), then create and share a note:

```sh
atlas --vault ./alpha init && atlas --vault ./beta init
atlas --vault ./alpha add n.md --title Note --body "start"
atlas --vault ./alpha sync --relay http://127.0.0.1:4000 --graph demo
atlas --vault ./beta sync --relay http://127.0.0.1:4000 --graph demo
```

Then open the same note live in two terminals:

```sh
# terminal A
atlas --vault ./alpha live n.md --relay ws://127.0.0.1:4000 --graph demo
# terminal B
atlas --vault ./beta live n.md --relay ws://127.0.0.1:4000 --graph demo
```

Type a line in either terminal and it appears in the other within a moment;
concurrent edits merge with no loss.

### Concurrent offline edits merge through a relay

Start a relay (`cargo run -p atlas-relay`) and run:

```sh
URL=http://127.0.0.1:4000

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
