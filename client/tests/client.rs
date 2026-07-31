//! Guarded-vault client: mutations update state and emit events.
#![expect(clippy::unwrap_used, reason = "tests read better with unwrap/expect")]

use atlas_client::{Client, Event};

#[test]
fn create_then_edit_emits_events_and_updates_state() {
    let dir = tempfile::tempdir().unwrap();
    let client = Client::open(dir.path()).unwrap();
    let mut events = client.subscribe();

    let id = client.create_note("n.md", "N", "hello").unwrap();
    let event = events.try_recv().unwrap();
    assert!(matches!(&event, Event::Note { .. }), "got {event:?}");
    if let Event::Note { id: got, body } = event {
        assert_eq!(got, id);
        assert_eq!(body, "hello");
    }

    client.edit_note(&id, "world").unwrap();
    assert_eq!(client.note_body(&id).unwrap(), "world");
    assert_eq!(client.list_notes().len(), 1);
}

#[test]
fn delete_removes_the_note() {
    let dir = tempfile::tempdir().unwrap();
    let client = Client::open(dir.path()).unwrap();
    let id = client.create_note("n.md", "N", "hello").unwrap();

    client.delete_note(&id).unwrap();
    assert!(client.list_notes().is_empty());
    assert!(client.note_body(&id).is_err());
}
