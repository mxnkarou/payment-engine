use std::fmt::Display;

use crate::{ClientId, TransactionId};

// Represents a single payment engine action
#[derive(Debug, Clone, Copy)]
pub enum PaymentCommand {
    Deposit {
        client: ClientId,
        tx: TransactionId,
        amount: f64,
    },

    Withdrawal {
        client: ClientId,
        tx: TransactionId,
        amount: f64,
    },

    Dispute {
        client: ClientId,
        tx: TransactionId,
    },

    Resolve {
        client: ClientId,
        tx: TransactionId,
    },

    Chargeback {
        client: ClientId,
        tx: TransactionId,
    },
}

#[derive(Debug, Clone)]
pub enum PaymentCommandParseError {
    MissingData(String),
    ParseError(String),
    NoSuchPaymentCommand(String),
    MissingHeader(String),
}

impl Display for PaymentCommandParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PaymentCommandParseError::MissingData(s) => {
                f.write_fmt(format_args!("Missing Data: {}", s))
            }
            PaymentCommandParseError::ParseError(s) => {
                f.write_fmt(format_args!("Parse Error: {}", s))
            }
            PaymentCommandParseError::NoSuchPaymentCommand(s) => {
                f.write_fmt(format_args!("No Such Payment Command: {}", s))
            }
            PaymentCommandParseError::MissingHeader(s) => {
                f.write_fmt(format_args!("Missing Header: {}", s))
            }
        }
    }
}

impl PaymentCommand {
    // using csv would be redundant when the parsing is this easy
    /// Parses a payment command from a single row of a CSV file.
    ///
    /// # Errors
    ///
    /// - `MissingData`: when the row is missing a required datum.
    /// - `ParseError`: when a cell cannot be parsed into a numerical type.
    /// - `NoSuchPaymentCommand`: when the `type` cell contains an invalid command.
    ///
    /// # Panics
    ///
    /// This function, in theory, should never panic.
    pub fn from_csv_line(s: &str) -> Result<Self, PaymentCommandParseError> {
        let split_string = s.split(',').map(str::trim).collect::<Vec<&str>>();

        if split_string.len() < 3 {
            return Err(PaymentCommandParseError::MissingData(
                "Too little data in the row.".to_string(),
            ));
        };

        match *split_string.get(0).unwrap() {
            "deposit" if split_string.len() < 4 => Err(PaymentCommandParseError::MissingData(
                "Too little data in the row.".to_string(),
            )),
            "deposit" => Ok(Self::Deposit {
                client: str::parse::<ClientId>(split_string[1]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse client id: {}",
                        e
                    ))
                })?,
                tx: str::parse::<TransactionId>(split_string[2]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse transaction id: {}",
                        e
                    ))
                })?,
                amount: str::parse::<f64>(split_string[3]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!("Could not parse amount: {}", e))
                })?,
            }),

            "withdrawal" if split_string.len() < 4 => Err(PaymentCommandParseError::MissingData(
                "Too little data in the row.".to_string(),
            )),
            "withdrawal" => Ok(Self::Withdrawal {
                client: str::parse::<ClientId>(split_string[1]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse client id: {}",
                        e
                    ))
                })?,
                tx: str::parse::<TransactionId>(split_string[2]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse transaction id: {}",
                        e
                    ))
                })?,
                amount: str::parse::<f64>(split_string[3]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!("Could not parse amount: {}", e))
                })?,
            }),

            "dispute" => Ok(Self::Dispute {
                client: str::parse::<ClientId>(split_string[1]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse client id: {}",
                        e
                    ))
                })?,
                tx: str::parse::<TransactionId>(split_string[2]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse transaction id: {}",
                        e
                    ))
                })?,
            }),

            "resolve" => Ok(Self::Resolve {
                client: str::parse::<ClientId>(split_string[1]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse client id: {}",
                        e
                    ))
                })?,
                tx: str::parse::<TransactionId>(split_string[2]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse transaction id: {}",
                        e
                    ))
                })?,
            }),

            "chargeback" => Ok(Self::Chargeback {
                client: str::parse::<ClientId>(split_string[1]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse client id: {}",
                        e
                    ))
                })?,
                tx: str::parse::<TransactionId>(split_string[2]).map_err(|e| {
                    PaymentCommandParseError::ParseError(format!(
                        "Could not parse transaction id: {}",
                        e
                    ))
                })?,
            }),

            s => Err(PaymentCommandParseError::NoSuchPaymentCommand(format!(
                "{} is not a valid payment command.",
                s
            ))),
        }
    }
}

/// This struct represents a `PaymentCommandParseError` occuring on a specific line.
#[derive(Debug, Clone)]
pub struct ParseError(pub usize, pub PaymentCommandParseError);

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[line {}] {}", self.0, self.1))
    }
}

// error returns the original error and the line on which it occurred
/// Parses a vector of commands from a CSV string.
///
/// # Errors
///
/// This function will return any error that it encounters when calling `PaymentCommand::from_csv_line`.
///
/// In addition, this function will return a `PaymentCommandParseError::MissingHeader` if it does not read the correct header.
pub fn parse_commands(s: &str) -> Result<Vec<PaymentCommand>, ParseError> {
    let mut commands = Vec::new();

    let mut iter = s.lines().enumerate();

    if let Some((_, line)) = iter.next() {
        let split_line = line.split(',').map(str::trim).collect::<Vec<_>>();

        if split_line[..] != ["type", "client", "tx", "amount"] {
            return Err(ParseError(
                0,
                PaymentCommandParseError::MissingHeader("Incorrect CSV header.".to_string()),
            ));
        }
    } else {
        return Err(ParseError(
            0,
            PaymentCommandParseError::MissingHeader("Missing CSV header.".to_string()),
        ));
    }

    for (i, line) in iter {
        let command = PaymentCommand::from_csv_line(line).map_err(|e| ParseError(i + 1, e))?;
        commands.push(command);
    }

    Ok(commands)
}