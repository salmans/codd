# codd

`codd` (named after [Edgar F. Codd](https://en.wikipedia.org/wiki/Edgar_F._Codd)) is a library for evaluating *typed* relational expressions in a minimal in-memory database. `codd` is primarily developed to support an implementation of [`razor`](https://github.com/salmans/rusty-razor) based on relational algebra but it is designed to be consistent with common concepts in database theory and may be used as a minimal general purpose in-memory database.

The implementation of database instances in `codd` is borrowed from [`datafrog`](https://github.com/rust-lang/datafrog):
* `Instance<T>` (`Variable<T>` in `datafrog`) contains tuples of type `T`,
* Incremental view maintenance is implemented by maintaining tuples of `Instance<T>` in three sets of `to_add` (candidate tuples to be inserted), `recent` (recently added tuples), and `stable` (old tuples that have been reflected in all views).

Unlike `datafrog`, `codd` distinguishes relation instances from views and offers the trait `Expression<T>` and types that implement `Expression<T>` to query the database.

The relational algebra and database terminology in `codd` is adopted from [Alice's book](http://webdam.inria.fr/Alice/).

## Build

`codd` is written in [Rust](https://www.rust-lang.org). You can use Rust 1.46.0 or newer to build the library:

```
git clone https://github.com/salmans/codd.git
cd codd
cargo build
```

## Example: music database

Use `codd` in your code:

```rust
use codd::{Database, Error};
```

Create a new database:

```rust
    let mut music = Database::new(); // music database
```

Add relations to the database:

```rust    
    // `musician`, `band` and `song` are `Relation` expressions.
    let musician = music.add_relation("musician")?;
    let band = music.add_relation("band")?;
    let song = music.add_relation("song")?;
```

Insert tuples (records) into your database relations:

```rust
    music.insert(
        &musician,
        vec![
            Musician {
                name: "John Petrucci".into(),
                band: Some("Dream Theater".into()),
                instruments: vec![Guitar],
            },
            Musician {
                name: "Taylor Swift".into(),
                band: None,
                instruments: vec![Vocals],
            },
            Musician {
                name: "Conor Mason".into(),
                band: Some("Nothing But Thieves".into()),
                instruments: vec![Vocals, Guitar],
            },
            ...
        ]
        .into(),
    )?;
    
    music.insert(
        &band,
        vec![
            Band {
                name: "Dream Theater".into(),
                genre: "Progressive Metal".into(),
            },
            Band {
                name: "Nothing But Thieves".into(),
                genre: "Alternative Rock".into(),
            },
            ...
        ]
        .into(),
    )?;

    music.insert(
        &song,
        vec![
            Song {
                title: "pull me under".into(),
                artist: Either::Right("Dream Theater".into()),
            },
            Song {
                title: "bad guy".into(),
                artist: Either::Left("Billie Eilish".into()),
            },
            Song {
                title: "excuse me".into(),
                artist: Either::Left("Nothing But Thieves".into()),
            },
            ...
        ]
        .into(),
    )?;
```

Construct query expressions and evaluate them in the database:

```rust
    use codd::{Project, Select};
    
    // Select all guitar players from the `musician` relation.
    let guitarists = Select::new(&musician, |m| m.instruments.contains(&Guitar));
    
    // Project the name of guitar players.
    let guitarist_names = Project::new(&guitarists, |g| g.name.to_string());

    assert_eq!(
        vec![
            "Alex Turner".to_string(),
            "Conor Mason".into(),
            "John Petrucci".into(),
        ],
        music.evaluate(&guitarist_names)?.into_tuples() // evaluate the query and get the results
    );
```

Here is a more complex query:

```rust
    use codd::Join;
    
    // Query the names of Dream Theater's members.
    let dt_members = Project::new(
        &Select::new(
            &Join::new( // joining `musician` and `band`
                &musician,
                &band,
                |m| m.band.clone(),       // the join key for `musician` (band name)
                |b| Some(b.name.clone()), // the join key for `band`
                |_, m, b| (m.name.to_string(), b.name.to_string()), // joining closure
            ),
            |m| m.1 == "Dream Theater", // selecting predicate
        ),
        |m| m.0.to_string(), // projecting clousre
    );

    assert_eq!(
        vec!["John Petrucci".to_string(), "Jordan Rudess".into()],
        music.evaluate(&dt_members)?.into_tuples()
    );
```

Store views of expressions:

```rust
    let dt_members_view = music.store_view(&dt_members)?; // view over Dream Theater member names
    let drummers_view =
        music.store_view(&Select::new(&musician, |m| m.instruments.contains(&Drums)))?; // view over drummers

    // inserting more tuples:
    music.insert(
        &musician,
        vec![
            Musician {
                name: "John Myung".into(),
                band: Some("Dream Theater".into()),
                instruments: vec![Guitar],
            },
            Musician {
                name: "Mike Mangini".into(),
                band: Some("Dream Theater".into()),
                instruments: vec![Drums],
            },
        ]
        .into(),
    )?;

    // views are up-to-date:
    assert_eq!(
        vec![
            Musician {
                name: "Lars Ulrich".into(),
                band: Some("Metallica".into()),
                instruments: vec![Drums]
            },
            Musician {
                name: "Mike Mangini".into(),
                band: Some("Dream Theater".into()),
                instruments: vec![Drums]
            }
        ],
        music.evaluate(&drummers_view)?.into_tuples()
    );
    assert_eq!(
        vec![
            "John Myung".to_string(),
            "John Petrucci".into(),
            "Jordan Rudess".into(),
            "Mike Mangini".into()
        ],
        music.evaluate(&dt_members_view)?.into_tuples()
    );

    Ok(())
}
```
