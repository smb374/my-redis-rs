use std::str::FromStr;

use anyhow::anyhow;
use nom::{
    IResult, Parser,
    bytes::streaming::{tag, take_until},
    character::streaming::{anychar, one_of},
    combinator::{map, map_res, peek},
};

#[derive(Debug, PartialEq, Clone)]
pub enum ProtocolData {
    SimpleString(String),
    SimpleError(String),
    Integer(i64),
    BulkString(String),
    Array(Vec<ProtocolData>),
    Null,
    Boolean(bool),
    Double(f64),
    BigNums(String),
    BulkError(String),
    Verbatim(String, String),
    Map(Vec<(ProtocolData, ProtocolData)>),
    Attributes(Vec<(ProtocolData, ProtocolData)>),
    Set(Vec<ProtocolData>),
    Push(Vec<ProtocolData>),
}

pub fn parse_protocol(s: &str) -> IResult<&str, ProtocolData> {
    match peek(anychar).parse(s)?.1 {
        '+' => parse_simple_string(s),
        '-' => parse_simple_error(s),
        ':' => parse_integer(s),
        '$' => parse_bulk_string(s),
        '*' => parse_array(s),
        '_' => parse_null(s),
        '#' => parse_boolean(s),
        ',' => parse_doubles(s),
        '(' => parse_bignum(s),
        '!' => parse_bulk_error(s),
        '=' => parse_verbatim(s),
        '%' => parse_map(s),
        '|' => parse_attributes(s),
        '~' => parse_set(s),
        '>' => parse_push(s),
        _ => unimplemented!(),
    }
}

pub fn encode_protocol(prot: ProtocolData) -> String {
    match prot {
        ProtocolData::SimpleString(s) => format!("+{}\r\n", &s),
        ProtocolData::SimpleError(s) => format!("-{}\r\n", &s),
        ProtocolData::Integer(v) => format!(":{}\r\n", v),
        ProtocolData::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
        _ => unimplemented!(),
    }
}

fn parse_line(s: &str) -> IResult<&str, &str> {
    map((take_until("\r\n"), tag("\r\n")), |(x, _)| x).parse(s)
}

fn parse_simple_string(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("+"), parse_line), |(_, x)| {
        ProtocolData::SimpleString(x.to_string())
    })
    .parse(s)
}

fn parse_simple_error(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("-"), parse_line), |(_, x)| {
        ProtocolData::SimpleError(x.to_string())
    })
    .parse(s)
}

fn parse_integer(s: &str) -> IResult<&str, ProtocolData> {
    map_res((tag(":"), parse_line), |(_, x)| {
        i64::from_str_radix(x, 10).map(|v| ProtocolData::Integer(v))
    })
    .parse(s)
}

fn parse_bulk_string(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("$"), parse_line, parse_line), |(_, _, x)| {
        ProtocolData::BulkString(x.to_string())
    })
    .parse(s)
}

fn parse_array_like(s: &str) -> IResult<&str, Vec<ProtocolData>> {
    let (s, len) = map_res(parse_line, |x| usize::from_str_radix(x, 10)).parse(s)?;
    let mut prots = Vec::with_capacity(len);
    let mut curr = s;
    for _ in 0..len {
        let (next, prot) = parse_protocol(curr)?;
        curr = next;
        prots.push(prot);
    }
    Ok((curr, prots))
}

fn parse_array(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("*"), parse_array_like), |(_, x)| {
        ProtocolData::Array(x)
    })
    .parse(s)
}

fn parse_null(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("_"), tag("\r\n")), |_| ProtocolData::Null).parse(s)
}

fn parse_boolean(s: &str) -> IResult<&str, ProtocolData> {
    map_res((tag("#"), one_of("tf")), |(_, c)| match c {
        't' => Ok(ProtocolData::Boolean(true)),
        'f' => Ok(ProtocolData::Boolean(false)),
        _ => Err(anyhow!("Unexpected character '{}' for boolean.", c)),
    })
    .parse(s)
}

fn parse_doubles(s: &str) -> IResult<&str, ProtocolData> {
    map_res((tag(","), parse_line), |(_, x)| {
        f64::from_str(x).map(ProtocolData::Double)
    })
    .parse(s)
}

fn parse_bignum(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("("), parse_line), |(_, x)| {
        ProtocolData::BigNums(x.to_owned())
    })
    .parse(s)
}

fn parse_bulk_error(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("!"), parse_line, parse_line), |(_, _, x)| {
        ProtocolData::BulkString(x.to_string())
    })
    .parse(s)
}

fn parse_verbatim(s: &str) -> IResult<&str, ProtocolData> {
    map(
        (tag("="), parse_line, take_until(":"), tag(":"), parse_line),
        |(_, _, encoding, _, data)| ProtocolData::Verbatim(encoding.to_owned(), data.to_owned()),
    )
    .parse(s)
}

fn parse_map_like(s: &str) -> IResult<&str, Vec<(ProtocolData, ProtocolData)>> {
    let (s, entries) = map_res(parse_line, |x| usize::from_str_radix(x, 10)).parse(s)?;
    let mut map = Vec::with_capacity(entries);
    let mut curr = s;

    for _ in 0..entries {
        let (next, tup) = (parse_protocol, parse_protocol).parse(s)?;
        map.push(tup);
        curr = next;
    }

    Ok((curr, map))
}

fn parse_map(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("%"), parse_map_like), |(_, x)| ProtocolData::Map(x)).parse(s)
}

fn parse_attributes(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("|"), parse_map_like), |(_, x)| {
        ProtocolData::Attributes(x)
    })
    .parse(s)
}

fn parse_set(s: &str) -> IResult<&str, ProtocolData> {
    map((tag("~"), parse_array_like), |(_, x)| ProtocolData::Set(x)).parse(s)
}

fn parse_push(s: &str) -> IResult<&str, ProtocolData> {
    map((tag(">"), parse_array_like), |(_, x)| ProtocolData::Push(x)).parse(s)
}
