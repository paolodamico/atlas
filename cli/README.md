# atlas-cli

A command-line client over `atlas-core`, and the vehicle for the offline-sync demo. Each vault is a directory (`--vault <dir>`, default `atlas-vault`).

## Notes

- Notes are addressed by id, a unique id prefix (as shown by `list`), or exact
  path.
- `sync` is one-directional: it pulls the other vault into this one and persists only this one, mirroring how a device applies changes received from a peer. To bring two vaults fully into sync, run it from each side.

## Walkthrough: concurrent offline edits merge without loss

```sh
atlas --vault ./alpha init
atlas --vault ./beta init

atlas --vault ./alpha add n.md --title Note --body "one two three"
atlas --vault ./beta sync ./alpha            # beta pulls the note from alpha

# Both devices edit the same note while "offline":
atlas --vault ./alpha edit n.md --body "ONE two three"
atlas --vault ./beta edit n.md --body "one two THREE"

atlas --vault ./alpha sync ./beta            # each side pulls the other
atlas --vault ./beta sync ./alpha

atlas --vault ./alpha show n.md           # ONE two THREE
atlas --vault ./beta show n.md           # ONE two THREE
```

## Run from the workspace

```sh
cargo run -p atlas-cli -- --vault ./A init
```
