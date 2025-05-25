use pgwire::api::{Type, results::FieldFormat};
use sqlx::{Column, Row, postgres::PgRow};

pub struct ResponseInfo<'a> {
    pub rows_n: usize,
    pub is_empty: bool,
    pub columns: Vec<ResponseColumn<'a>>,
}

pub struct ResponseColumn<'a> {
    pub name: &'a str,
    pub field_type: Type,
    pub format: FieldFormat,
}

impl<'a> ResponseInfo<'a> {
    pub fn from_rows(rows: &'a [PgRow]) -> ResponseInfo<'a> {
        let n = rows.len();
        let first_col = rows.first();

        let cols = match first_col {
            Some(rows) => rows
                .columns()
                .iter()
                .map(|c| {
                    let field_type =
                        Type::from_oid(c.relation_id().map_or_else(|| 0, |id| id.0) as u32)
                            .unwrap_or_else(|| Type::VARCHAR);

                    let format = match field_type {
                        Type::BOOL => FieldFormat::Binary,
                        _ => FieldFormat::Text,
                    };

                    ResponseColumn {
                        name: c.name(),
                        field_type: field_type,
                        format: format,
                    }
                })
                .collect::<Vec<ResponseColumn<'a>>>(),
            None => vec![],
        };

        Self {
            rows_n: n,
            is_empty: n == 0,
            columns: cols,
        }
    }
}
