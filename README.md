# codd

`codd` (named after [Edgar F. Codd](https://en.wikipedia.org/wiki/Edgar_F._Codd)) is a library for evaluating *typed* relational expressions in a monotonically growing minimal database in memory. `codd` is primarily developed to support an implementation of [`razor`](https://github.com/salmans/rusty-razor) based on relational algebra, however, its design is consistent with common concepts of database theory and may be used as a minimal general purpose database.

The implementation of database instances in `codd` is borrowed from [`datafrog`](https://github.com/rust-lang/datafrog):
* `Instance<T>` (`Variable<T>` in `datafrog`) contains tuples of type `T`,
* Incremental view maintenance is implemented by maintaining tuples of `Instance<T>` in three sets of `to_add` (candidate tuples to be inserted), `recent` (recently added tuples), and `stable` (old tuples that have been reflected in all views).

In contrast, `codd` distinguishes relation instances from views and offers the trait `Expression<T>` and types that implement `Expression<T>` to query a database.

The relational algebra and database terminology in `codd` is adopted from [Alice's book](http://webdam.inria.fr/Alice/).

## Build

`codd` is written in [Rust](https://www.rust-lang.org). You can use Rust 1.46.0 or newer to build the library:

```
git clone https://github.com/salmans/codd.git
cd codd
cargo build
```

## Example: [music](https://github.com/salmans/codd/blob/master/core/examples/music.rs)

Add `codd` to your project dependencies in Cargo.toml:

```
[dependencies]
codd = "0.1"
```

Use `codd` in your code:

```rust
use codd::{Database, Error, Expression};
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

Insert tuples (records) to your database relations:

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
            // more tuples...
        ]
        .into(),
    )?;
    
    // add tuples to other relations...
```

Construct query expressions and evaluate them in the database:

```rust

    let guitarist_name = musician
        .builder()
        .select(|m| m.instruments.contains(&Guitar))
        .project(|g| g.name.to_string())
        .build();

    assert_eq!(
        vec![
            "Alex Turner".to_string(),
            "Conor Mason".into(),
            "John Petrucci".into(),
        ],
        music.evaluate(&guitarist_name)?.into_tuples() // evaluate the query and get the results
    );
```

Here is a more complex query:

```rust
    let dt_member = musician
        .builder()
        .with_key(|m| m.band.clone())
            // use `band` as the join key for `musician`
        .join(band.builder().with_key(|b| Some(b.name.clone()))) 
            // join with `band` with `name` as the join key
        .on(|_, m, b| (m.name.to_string(), b.name.to_string()))
            // combine tuples of `musician` and `band` in a new relation
        .select(|m| m.1 == "Dream Theater")
        .project(|m| m.0.to_string())
        .build();

    assert_eq!(
        vec!["John Petrucci".to_string(), "Jordan Rudess".into()],
        music.evaluate(&dt_member)?.into_tuples()
    );
```

Store views of expressions:

```rust
    let dt_member_view = music.store_view(dt_members)?; // view on `dt_member`
    let drummer_view = music.store_view(                // drummers view
        musician
            .builder()
            .select(|m| m.instruments.contains(&Drums))
            .build(),
    )?;

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
        music.evaluate(&drummer_view)?.into_tuples()
    );
    assert_eq!(
        vec![
            "John Myung".to_string(),
            "John Petrucci".into(),
            "Jordan Rudess".into(),
            "Mike Mangini".into()
        ],
        music.evaluate(&dt_member_view)?.into_tuples()
    );

    Ok(())
}
```
