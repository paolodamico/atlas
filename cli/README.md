# atlas-cli

A command-line client over `atlas-core`, and the vehicle for the offline-sync demo. Each vault is a directory (`--vault <dir>`, default `atlas-vault`).

## Notes

- Notes are addressed by id, a unique id prefix (as shown by `list`), or exact
  path.
- `sync` is one-directional: it pulls the other vault into this one and persists only this one, mirroring how a device applies changes received from a peer. To bring two vaults fully into sync, run it from each side.

## Walkthrough: concurrent offline edits merge without loss

```sh
atlas --vault ./A init
atlas --vault ./B init

atlas --vault ./A add n.md --title Note --body "one two three"
atlas --vault ./B sync ./A            # B pulls the note from A

# Both devices edit the same note while "offline":
atlas --vault ./A edit n.md --body "ONE two three"
atlas --vault ./B edit n.md --body "one two THREE"

atlas --vault ./A sync ./B            # each side pulls the other
atlas --vault ./B sync ./A

atlas --vault ./A show n.md           # ONE two THREE
atlas --vault ./B show n.md           # ONE two THREE
```

## Run from the workspace

```sh
cargo run -p atlas-cli -- --vault ./A init
```
