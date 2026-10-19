# codd

[`codd`](https://en.wikipedia.org/wiki/Edgar_F._Codd) is a library for evaluating *typed* relational expressions in a minimal in-memory database with incrementally maintained views. `codd` is primarily developed to support an implementation of [`razor`](https://github.com/salmans/rusty-razor) based on relational algebra, however, its design is consistent with common concepts of database theory and may be used as a minimal general purpose database.

Storage and incremental view maintenance follow [`datafrog`](https://github.com/rust-lang/datafrog) and [DBSP](https://github.com/feldera/feldera):
* Every relation and view is an `Instance<T>` holding sorted, immutable batches of *weighted* tuples (a Z-set). Relations have set semantics (weights are kept at zero or one, so `insert` of a present tuple and `retract` of an absent one are no-ops); views keep the number of derivations of each tuple as its weight.
* Changes arrive in `to_add`, are folded in order into one `recent` change, propagated to dependent views as weighted deltas, and then merged into the `stable` batches. Linear operators pass deltas through, bilinear operators (join, product, intersect) use `ΔL⋈R + L⋈ΔR + ΔL⋈ΔR`, and the non-linear ones (`Difference` as an antijoin, `Distinct`) recompute the affected tuples from the current counts of their operands.
* Batches are shared between a database and its clones, so cloning is cheap and clones diverge independently, including by retraction.

In contrast, `codd` distinguishes relation instances from views and offers the trait `Expression<T>` and types that implement `Expression<T>` to query a database.

The relational algebra and database terminology in `codd` is adopted from [Alice's book](http://webdam.inria.fr/Alice/).

## Build

`codd` is written in [Rust](https://www.rust-lang.org). You can use Rust 1.54.0 or newer to build the library:

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
