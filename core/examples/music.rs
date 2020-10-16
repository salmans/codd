use codd::{Database, Error, Expression};
use either::Either;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]

enum Instrument {
    Guitar,
    Piano,
    Keyboard,
    Drums,
    Vocals,
}
use Instrument::*;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Musician {
    name: String,
    band: Option<String>,
    instruments: Vec<Instrument>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Band {
    name: String,
    genre: String,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct Song {
    title: String,
    artist: Either<String /* musician */, String /* band */>,
}

fn main() -> Result<(), Error> {
    let mut music = Database::new();
    let musician = music.add_relation("musician")?;
    let band = music.add_relation("band")?;
    let song = music.add_relation("song")?;
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
            Musician {
                name: "Stevie Wonder".into(),
                band: None,
                instruments: vec![Vocals, Piano],
            },
            Musician {
                name: "Jordan Rudess".into(),
                band: Some("Dream Theater".into()),
                instruments: vec![Keyboard],
            },
            Musician {
                name: "Alex Turner".into(),
                band: Some("Arctic Monkeys".into()),
                instruments: vec![Vocals, Guitar, Piano],
            },
            Musician {
                name: "Billie Eilish".into(),
                band: None,
                instruments: vec![Vocals, Piano],
            },
            Musician {
                name: "Lars Ulrich".into(),
                band: Some("Metallica".into()),
                instruments: vec![Drums],
            },
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
            Band {
                name: "Metallica".into(),
                genre: "Heavy Metal".into(),
            },
            Band {
                name: "Arctic Monkeys".into(),
                genre: "Indie Rock".into(),
            },
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
            Song {
                title: "enter sandman".into(),
                artist: Either::Right("Metallica".into()),
            },
            Song {
                title: "panic attack".into(),
                artist: Either::Right("Dream Theater".into()),
            },
            Song {
                title: "shake it off".into(),
                artist: Either::Left("Taylor Swift".into()),
            },
            Song {
                title: "r u mine".into(),
                artist: Either::Right("Artcic Monkeys".into()),
            },
            Song {
                title: "as I am".into(),
                artist: Either::Right("Dream Theater".into()),
            },
        ]
        .into(),
    )?;

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
        music.evaluate(&guitarist_name)?.into_tuples()
    );

    let dt_member = musician
        .builder()
        .with_key(|m| m.band.clone())
        .join(band.builder().with_key(|b| Some(b.name.clone())))
        .on(|_, m, b| (m.name.to_string(), b.name.to_string()))
        .select(|m| m.1 == "Dream Theater")
        .project(|m| m.0.to_string())
        .build();

    assert_eq!(
        vec!["John Petrucci".to_string(), "Jordan Rudess".into()],
        music.evaluate(&dt_member)?.into_tuples()
    );

    let dt_member_view = music.store_view(dt_member)?;
    let drummer_view = music.store_view(
        musician
            .builder()
            .select(|m| m.instruments.contains(&Drums))
            .build(),
    )?;

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
