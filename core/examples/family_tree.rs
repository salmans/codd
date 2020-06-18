use anyhow::Result;
use relalg::{relalg, Database, Expression};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct Person {
    id: u32,
    father_id: Option<u32>,
    mother_id: Option<u32>,
    name: String,
}

fn main() -> Result<()> {
    let mut got = Database::new();
    let person = got.new_relation::<Person>("Person");
    person.insert(
        vec![
            Person {
                id: 1,
                name: "Arya Stark".to_string(),
                father_id: Some(2),
                mother_id: Some(3),
            },
            Person {
                id: 2,
                name: "Eddard Stark".to_string(),
                father_id: None,
                mother_id: None,
            },
            Person {
                id: 3,
                name: "Catelyn Stark".to_string(),
                father_id: None,
                mother_id: None,
            },
            Person {
                id: 4,
                name: "John Snow".to_string(),
                father_id: None,
                mother_id: None,
            },
        ]
        .into(),
        &got,
    )?;

    let ariyas_father = relalg! {
        select [|p| (p.father_id.unwrap(), ())] from (person)
        where
            [|p| p.father_id.is_some() && p.name == "Arya Stark"]

    };

    let persons_name = relalg! {
        select [|p| (p.id, p.name.clone())] from (person)
    };

    let fathers_name = relalg! {
        select * from (
            (ariyas_father) join (persons_name) on [|_, _, name| name.clone()]
        )
    };

    let names = fathers_name.evaluate(&got);

    for name in names.iter() {
        println!("{:?}", name);
    }

    Ok(())
}
