//! A small SQL-arithmetic expression evaluator.
//!
//! The Java `doGeneralFuelRatio` evaluates each `generalFuelRatioExpression`
//! row by pasting its `fuelEffectRatioExpression` text straight into a
//! `select (EXPRESSION) from fuelFormulation` statement — MariaDB then
//! computes it against each fuel-formulation row. Porting that path off the
//! database means evaluating the same expression text in Rust.
//!
//! This module is a self-contained recursive-descent evaluator for the
//! arithmetic subset of MariaDB expression syntax those columns use:
//! numeric literals, `fuelFormulation` column references, the operators
//! `+ - * / %`, unary `-`, comparisons (`= <> != < <= > >=`), the boolean
//! operators `and`/`or`/`not`, parentheses, and the scalar functions
//! `if`, `pow`/`power`, `exp`, `log`/`ln`, `sqrt`, `abs`, `least`,
//! `greatest`.
//!
//! # Fidelity notes
//!
//! * Every value is an `f64`. MariaDB evaluates these expressions in
//!   `DOUBLE`; the `fuelFormulation` columns are `FLOAT` (32-bit), so the
//!   caller stores them as `f32` and the [`VariableSource`] promotes them
//!   to `f64` here — reproducing "read a `FLOAT` column, compute in
//!   `DOUBLE`".
//! * MariaDB rounds an integer/integer division such as `(5/9)` to a
//!   `DECIMAL` before promoting to `DOUBLE` (`0.5556`, not `0.55555…`).
//!   This evaluator divides in plain `f64`. A `fuelEffectRatioExpression`
//!   that divides two integer *literals* would diverge by ~1e-4; one that
//!   divides by a column does not (the column is already `DOUBLE`). The
//!   bug-compatibility decision belongs to Task 44 (generator integration
//!   validation), matching the Task 41 / Task 33 precedent.
//! * Booleans follow the MariaDB convention: a comparison yields `1.0` or
//!   `0.0`, and any non-zero value is "true".

use thiserror::Error;

/// Supplies values for the identifiers in an [`Expression`].
///
/// Implemented by [`FuelFormulation`](super::model::FuelFormulation), whose
/// columns are the variables a `fuelEffectRatioExpression` may reference.
pub trait VariableSource {
    /// Return the value bound to `name`, or `None` if the identifier is
    /// not known. Implementations should match case-insensitively, since
    /// MariaDB column names are case-insensitive.
    fn variable(&self, name: &str) -> Option<f64>;
}

/// A parse or evaluation failure.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ExpressionError {
    /// The expression text held a character the tokenizer does not accept.
    #[error("unexpected character {0:?} in fuel-effect expression")]
    UnexpectedChar(char),
    /// A numeric literal did not parse as a number.
    #[error("malformed number {0:?} in fuel-effect expression")]
    BadNumber(String),
    /// The expression ended while more input was expected.
    #[error("unexpected end of fuel-effect expression")]
    UnexpectedEnd,
    /// A token appeared where the grammar did not allow it.
    #[error("unexpected token {0:?} in fuel-effect expression")]
    UnexpectedToken(String),
    /// A function call named a function this evaluator does not implement.
    #[error("unknown function {0:?} in fuel-effect expression")]
    UnknownFunction(String),
    /// A function call had the wrong number of arguments.
    #[error("function {func}() expects {expected} argument(s), got {got}")]
    ArgCount {
        /// The function name.
        func: String,
        /// A human-readable description of the accepted arity.
        expected: String,
        /// The number of arguments supplied.
        got: usize,
    },
    /// An identifier was not resolved by the [`VariableSource`].
    #[error("unknown variable {0:?} in fuel-effect expression")]
    UnknownVariable(String),
}

/// A lexical token.
#[derive(Debug, Clone, PartialEq)]
enum Token {
    Number(f64),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    LParen,
    RParen,
    Comma,
}

impl Token {
    /// A short label for error messages.
    fn label(&self) -> String {
        match self {
            Token::Number(n) => n.to_string(),
            Token::Ident(s) => s.clone(),
            Token::Plus => "+".into(),
            Token::Minus => "-".into(),
            Token::Star => "*".into(),
            Token::Slash => "/".into(),
            Token::Percent => "%".into(),
            Token::Eq => "=".into(),
            Token::Ne => "<>".into(),
            Token::Lt => "<".into(),
            Token::Le => "<=".into(),
            Token::Gt => ">".into(),
            Token::Ge => ">=".into(),
            Token::LParen => "(".into(),
            Token::RParen => ")".into(),
            Token::Comma => ",".into(),
        }
    }
}

/// Split expression text into tokens.
fn tokenize(text: &str) -> Result<Vec<Token>, ExpressionError> {
    let chars: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_digit()
            || (c == '.' && chars.get(i + 1).is_some_and(char::is_ascii_digit))
        {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            // Optional exponent: e/E followed by an optional sign and digits.
            if i < chars.len() && (chars[i] == 'e' || chars[i] == 'E') {
                let mut j = i + 1;
                if j < chars.len() && (chars[j] == '+' || chars[j] == '-') {
                    j += 1;
                }
                if j < chars.len() && chars[j].is_ascii_digit() {
                    i = j;
                    while i < chars.len() && chars[i].is_ascii_digit() {
                        i += 1;
                    }
                }
            }
            let literal: String = chars[start..i].iter().collect();
            let value = literal
                .parse::<f64>()
                .map_err(|_| ExpressionError::BadNumber(literal.clone()))?;
            tokens.push(Token::Number(value));
        } else if c.is_ascii_alphabetic() || c == '_' {
            let start = i;
            while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            tokens.push(Token::Ident(chars[start..i].iter().collect()));
        } else {
            // Operators and punctuation, longest match first.
            let two: Option<Token> = match (c, chars.get(i + 1)) {
                ('<', Some('=')) => Some(Token::Le),
                ('>', Some('=')) => Some(Token::Ge),
                ('<', Some('>')) => Some(Token::Ne),
                ('!', Some('=')) => Some(Token::Ne),
                ('=', Some('=')) => Some(Token::Eq),
                _ => None,
            };
            if let Some(tok) = two {
                tokens.push(tok);
                i += 2;
                continue;
            }
            let one = match c {
                '+' => Token::Plus,
                '-' => Token::Minus,
                '*' => Token::Star,
                '/' => Token::Slash,
                '%' => Token::Percent,
                '=' => Token::Eq,
                '<' => Token::Lt,
                '>' => Token::Gt,
                '(' => Token::LParen,
                ')' => Token::RParen,
                ',' => Token::Comma,
                other => return Err(ExpressionError::UnexpectedChar(other)),
            };
            tokens.push(one);
            i += 1;
        }
    }
    Ok(tokens)
}

/// A scalar function recognised in `primary`-position calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Func {
    /// `if(cond, then, otherwise)` — only the taken branch is evaluated.
    If,
    /// `pow(base, exp)` / `power(base, exp)`.
    Pow,
    /// `exp(x)`.
    Exp,
    /// `log(x)` natural log, or `log(base, x)`.
    Log,
    /// `ln(x)` natural log.
    Ln,
    /// `sqrt(x)`.
    Sqrt,
    /// `abs(x)`.
    Abs,
    /// `least(a, b, …)`.
    Least,
    /// `greatest(a, b, …)`.
    Greatest,
}

impl Func {
    /// Resolve a (case-insensitive) function name.
    fn from_name(name: &str) -> Option<Func> {
        Some(match name.to_ascii_lowercase().as_str() {
            "if" => Func::If,
            "pow" | "power" => Func::Pow,
            "exp" => Func::Exp,
            "log" => Func::Log,
            "ln" => Func::Ln,
            "sqrt" => Func::Sqrt,
            "abs" => Func::Abs,
            "least" => Func::Least,
            "greatest" => Func::Greatest,
            _ => return None,
        })
    }

    /// Validate the argument count, returning a description of the
    /// accepted arity on mismatch.
    fn check_arity(self, name: &str, got: usize) -> Result<(), ExpressionError> {
        let ok = match self {
            Func::If => got == 3,
            Func::Pow => got == 2,
            Func::Log => got == 1 || got == 2,
            Func::Exp | Func::Ln | Func::Sqrt | Func::Abs => got == 1,
            Func::Least | Func::Greatest => got >= 1,
        };
        if ok {
            return Ok(());
        }
        let expected = match self {
            Func::If => "3",
            Func::Pow => "2",
            Func::Log => "1 or 2",
            Func::Exp | Func::Ln | Func::Sqrt | Func::Abs => "1",
            Func::Least | Func::Greatest => "at least 1",
        };
        Err(ExpressionError::ArgCount {
            func: name.to_string(),
            expected: expected.to_string(),
            got,
        })
    }
}

/// Binary operators, in the precedence bands the parser walks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Rem,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    And,
    Or,
}

/// A node of the parsed expression tree.
#[derive(Debug, Clone, PartialEq)]
enum Node {
    Number(f64),
    Variable(String),
    Neg(Box<Node>),
    Not(Box<Node>),
    Binary(BinOp, Box<Node>, Box<Node>),
    Call(Func, Vec<Node>),
}

/// A parsed, reusable expression.
///
/// Parse once with [`Expression::parse`], then evaluate against many
/// [`VariableSource`]s — exactly the access pattern of `doGeneralFuelRatio`,
/// which applies one expression to every fuel formulation of a fuel type.
#[derive(Debug, Clone, PartialEq)]
pub struct Expression {
    root: Node,
}

impl Expression {
    /// Parse expression text into a reusable tree.
    ///
    /// # Errors
    ///
    /// Returns an [`ExpressionError`] if the text does not tokenize or does
    /// not parse as a complete expression.
    pub fn parse(text: &str) -> Result<Expression, ExpressionError> {
        let tokens = tokenize(text)?;
        let mut parser = Parser { tokens, pos: 0 };
        let root = parser.parse_expression()?;
        if parser.pos != parser.tokens.len() {
            return Err(ExpressionError::UnexpectedToken(
                parser.tokens[parser.pos].label(),
            ));
        }
        Ok(Expression { root })
    }

    /// Evaluate the expression against `vars`.
    ///
    /// # Errors
    ///
    /// Returns [`ExpressionError::UnknownVariable`] if the expression
    /// references an identifier `vars` does not resolve, or
    /// [`ExpressionError::ArgCount`] for a defended-against arity mismatch.
    pub fn evaluate(&self, vars: &impl VariableSource) -> Result<f64, ExpressionError> {
        eval(&self.root, vars)
    }
}

/// Recursive-descent parser over a token slice.
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn bump(&mut self) -> Option<Token> {
        let tok = self.tokens.get(self.pos).cloned();
        if tok.is_some() {
            self.pos += 1;
        }
        tok
    }

    /// True when the next token is the keyword `word` (case-insensitive).
    fn peek_keyword(&self, word: &str) -> bool {
        matches!(self.peek(), Some(Token::Ident(s)) if s.eq_ignore_ascii_case(word))
    }

    /// expression := or
    fn parse_expression(&mut self) -> Result<Node, ExpressionError> {
        self.parse_or()
    }

    /// or := and ( "or" and )*
    fn parse_or(&mut self) -> Result<Node, ExpressionError> {
        let mut lhs = self.parse_and()?;
        while self.peek_keyword("or") {
            self.pos += 1;
            let rhs = self.parse_and()?;
            lhs = Node::Binary(BinOp::Or, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    /// and := not ( "and" not )*
    fn parse_and(&mut self) -> Result<Node, ExpressionError> {
        let mut lhs = self.parse_not()?;
        while self.peek_keyword("and") {
            self.pos += 1;
            let rhs = self.parse_not()?;
            lhs = Node::Binary(BinOp::And, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    /// not := "not" not | comparison
    fn parse_not(&mut self) -> Result<Node, ExpressionError> {
        if self.peek_keyword("not") {
            self.pos += 1;
            return Ok(Node::Not(Box::new(self.parse_not()?)));
        }
        self.parse_comparison()
    }

    /// comparison := additive ( cmp_op additive )?
    fn parse_comparison(&mut self) -> Result<Node, ExpressionError> {
        let lhs = self.parse_additive()?;
        let op = match self.peek() {
            Some(Token::Eq) => BinOp::Eq,
            Some(Token::Ne) => BinOp::Ne,
            Some(Token::Lt) => BinOp::Lt,
            Some(Token::Le) => BinOp::Le,
            Some(Token::Gt) => BinOp::Gt,
            Some(Token::Ge) => BinOp::Ge,
            _ => return Ok(lhs),
        };
        self.pos += 1;
        let rhs = self.parse_additive()?;
        Ok(Node::Binary(op, Box::new(lhs), Box::new(rhs)))
    }

    /// additive := multiplicative ( ("+"|"-") multiplicative )*
    fn parse_additive(&mut self) -> Result<Node, ExpressionError> {
        let mut lhs = self.parse_multiplicative()?;
        loop {
            let op = match self.peek() {
                Some(Token::Plus) => BinOp::Add,
                Some(Token::Minus) => BinOp::Sub,
                _ => break,
            };
            self.pos += 1;
            let rhs = self.parse_multiplicative()?;
            lhs = Node::Binary(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    /// multiplicative := unary ( ("*"|"/"|"%") unary )*
    fn parse_multiplicative(&mut self) -> Result<Node, ExpressionError> {
        let mut lhs = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Some(Token::Star) => BinOp::Mul,
                Some(Token::Slash) => BinOp::Div,
                Some(Token::Percent) => BinOp::Rem,
                _ => break,
            };
            self.pos += 1;
            let rhs = self.parse_unary()?;
            lhs = Node::Binary(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }

    /// unary := ("-"|"+") unary | primary
    fn parse_unary(&mut self) -> Result<Node, ExpressionError> {
        match self.peek() {
            Some(Token::Minus) => {
                self.pos += 1;
                Ok(Node::Neg(Box::new(self.parse_unary()?)))
            }
            Some(Token::Plus) => {
                // Unary plus is a no-op.
                self.pos += 1;
                self.parse_unary()
            }
            _ => self.parse_primary(),
        }
    }

    /// primary := Number | "(" expression ")" | Ident [ "(" args ")" ]
    fn parse_primary(&mut self) -> Result<Node, ExpressionError> {
        match self.bump() {
            None => Err(ExpressionError::UnexpectedEnd),
            Some(Token::Number(n)) => Ok(Node::Number(n)),
            Some(Token::LParen) => {
                let inner = self.parse_expression()?;
                match self.bump() {
                    Some(Token::RParen) => Ok(inner),
                    Some(other) => Err(ExpressionError::UnexpectedToken(other.label())),
                    None => Err(ExpressionError::UnexpectedEnd),
                }
            }
            Some(Token::Ident(name)) => {
                if matches!(self.peek(), Some(Token::LParen)) {
                    self.pos += 1; // consume "("
                    let args = self.parse_args()?;
                    let Some(func) = Func::from_name(&name) else {
                        return Err(ExpressionError::UnknownFunction(name));
                    };
                    func.check_arity(&name, args.len())?;
                    Ok(Node::Call(func, args))
                } else if matches!(name.to_ascii_lowercase().as_str(), "and" | "or" | "not") {
                    // A boolean keyword cannot stand as an operand.
                    Err(ExpressionError::UnexpectedToken(name))
                } else {
                    Ok(Node::Variable(name))
                }
            }
            Some(other) => Err(ExpressionError::UnexpectedToken(other.label())),
        }
    }

    /// args := [ expression ( "," expression )* ] ")"
    fn parse_args(&mut self) -> Result<Vec<Node>, ExpressionError> {
        let mut args = Vec::new();
        if matches!(self.peek(), Some(Token::RParen)) {
            self.pos += 1;
            return Ok(args);
        }
        loop {
            args.push(self.parse_expression()?);
            match self.bump() {
                Some(Token::Comma) => {}
                Some(Token::RParen) => break,
                Some(other) => return Err(ExpressionError::UnexpectedToken(other.label())),
                None => return Err(ExpressionError::UnexpectedEnd),
            }
        }
        Ok(args)
    }
}

/// MariaDB truthiness: any non-zero, non-NaN value is true.
fn truthy(value: f64) -> bool {
    value != 0.0 && !value.is_nan()
}

/// `1.0`/`0.0` for a boolean, matching a MariaDB comparison result.
fn boolean(value: bool) -> f64 {
    f64::from(u8::from(value))
}

/// Evaluate a parsed node against `vars`.
fn eval(node: &Node, vars: &impl VariableSource) -> Result<f64, ExpressionError> {
    match node {
        Node::Number(n) => Ok(*n),
        Node::Variable(name) => vars
            .variable(name)
            .ok_or_else(|| ExpressionError::UnknownVariable(name.clone())),
        Node::Neg(inner) => Ok(-eval(inner, vars)?),
        Node::Not(inner) => Ok(boolean(!truthy(eval(inner, vars)?))),
        Node::Binary(op, lhs, rhs) => {
            let a = eval(lhs, vars)?;
            let b = eval(rhs, vars)?;
            Ok(match op {
                BinOp::Add => a + b,
                BinOp::Sub => a - b,
                BinOp::Mul => a * b,
                // See the module-level fidelity note on integer division.
                BinOp::Div => a / b,
                BinOp::Rem => a % b,
                BinOp::Eq => boolean(a == b),
                BinOp::Ne => boolean(a != b),
                BinOp::Lt => boolean(a < b),
                BinOp::Le => boolean(a <= b),
                BinOp::Gt => boolean(a > b),
                BinOp::Ge => boolean(a >= b),
                BinOp::And => boolean(truthy(a) && truthy(b)),
                BinOp::Or => boolean(truthy(a) || truthy(b)),
            })
        }
        Node::Call(func, args) => eval_call(*func, args, vars),
    }
}

/// Evaluate a function call.
fn eval_call(
    func: Func,
    args: &[Node],
    vars: &impl VariableSource,
) -> Result<f64, ExpressionError> {
    // `if` is lazy: only the taken branch is evaluated.
    if func == Func::If {
        let [cond, then, otherwise] = args else {
            return Err(arity_error("if", "3", args.len()));
        };
        return if truthy(eval(cond, vars)?) {
            eval(then, vars)
        } else {
            eval(otherwise, vars)
        };
    }

    let values: Vec<f64> = args
        .iter()
        .map(|a| eval(a, vars))
        .collect::<Result<_, _>>()?;
    match (func, values.as_slice()) {
        (Func::Pow, [base, exp]) => Ok(base.powf(*exp)),
        (Func::Exp, [x]) => Ok(x.exp()),
        (Func::Ln, [x]) => Ok(x.ln()),
        (Func::Log, [x]) => Ok(x.ln()),
        (Func::Log, [base, x]) => Ok(x.log(*base)),
        (Func::Sqrt, [x]) => Ok(x.sqrt()),
        (Func::Abs, [x]) => Ok(x.abs()),
        (Func::Least, [first, rest @ ..]) => Ok(rest.iter().fold(*first, |m, v| m.min(*v))),
        (Func::Greatest, [first, rest @ ..]) => Ok(rest.iter().fold(*first, |m, v| m.max(*v))),
        // Arity is checked at parse time; this arm only guards against a
        // future grammar change leaving a malformed `Call` node.
        (other, _) => Err(arity_error(
            func_name(other),
            "a different count",
            values.len(),
        )),
    }
}

/// The lowercase spelling used in error messages.
fn func_name(func: Func) -> &'static str {
    match func {
        Func::If => "if",
        Func::Pow => "pow",
        Func::Exp => "exp",
        Func::Log => "log",
        Func::Ln => "ln",
        Func::Sqrt => "sqrt",
        Func::Abs => "abs",
        Func::Least => "least",
        Func::Greatest => "greatest",
    }
}

/// Build an [`ExpressionError::ArgCount`].
fn arity_error(func: &str, expected: &str, got: usize) -> ExpressionError {
    ExpressionError::ArgCount {
        func: func.to_string(),
        expected: expected.to_string(),
        got,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// A trivial [`VariableSource`] backed by a name→value map.
    struct Vars(BTreeMap<String, f64>);

    impl Vars {
        fn new(pairs: &[(&str, f64)]) -> Self {
            Vars(pairs.iter().map(|(k, v)| ((*k).to_string(), *v)).collect())
        }
    }

    impl VariableSource for Vars {
        fn variable(&self, name: &str) -> Option<f64> {
            self.0
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(name))
                .map(|(_, v)| *v)
        }
    }

    fn eval_str(text: &str, vars: &Vars) -> f64 {
        Expression::parse(text)
            .expect("parses")
            .evaluate(vars)
            .expect("evaluates")
    }

    #[test]
    fn evaluates_the_test_fixture_expressions() {
        // The two expressions testDoGeneralFuelRatio inserts, with MTBEVolume=10.
        let vars = Vars::new(&[("MTBEVolume", 10.0)]);
        assert_eq!(eval_str("MTBEVolume+7", &vars), 17.0);
        assert_eq!(eval_str("MTBEVolume*2", &vars), 20.0);
    }

    #[test]
    fn arithmetic_precedence_and_parentheses() {
        let vars = Vars::new(&[]);
        assert_eq!(eval_str("2+3*4", &vars), 14.0);
        assert_eq!(eval_str("(2+3)*4", &vars), 20.0);
        assert_eq!(eval_str("-3+10", &vars), 7.0);
        assert_eq!(eval_str("10-2-3", &vars), 5.0);
        assert_eq!(eval_str("2*3/4", &vars), 1.5);
        assert_eq!(eval_str("7 % 4", &vars), 3.0);
    }

    #[test]
    fn case_insensitive_identifiers() {
        let vars = Vars::new(&[("RVP", 8.5)]);
        assert_eq!(eval_str("rvp * 2", &vars), 17.0);
        assert_eq!(eval_str("RVP - 0.5", &vars), 8.0);
    }

    #[test]
    fn comparisons_and_booleans() {
        let vars = Vars::new(&[("x", 5.0)]);
        assert_eq!(eval_str("x > 3", &vars), 1.0);
        assert_eq!(eval_str("x < 3", &vars), 0.0);
        assert_eq!(eval_str("x = 5", &vars), 1.0);
        assert_eq!(eval_str("x <> 5", &vars), 0.0);
        assert_eq!(eval_str("x >= 5 and x <= 10", &vars), 1.0);
        assert_eq!(eval_str("x < 0 or x > 4", &vars), 1.0);
        assert_eq!(eval_str("not (x > 100)", &vars), 1.0);
    }

    #[test]
    fn scalar_functions() {
        let vars = Vars::new(&[("x", 9.0)]);
        assert_eq!(eval_str("sqrt(x)", &vars), 3.0);
        assert_eq!(eval_str("abs(0-x)", &vars), 9.0);
        assert_eq!(eval_str("pow(2,10)", &vars), 1024.0);
        assert_eq!(eval_str("power(3,2)", &vars), 9.0);
        assert_eq!(eval_str("least(4, x, 1)", &vars), 1.0);
        assert_eq!(eval_str("greatest(4, x, 1)", &vars), 9.0);
        assert!((eval_str("exp(0)", &vars) - 1.0).abs() < 1e-12);
        assert!((eval_str("ln(exp(1))", &vars) - 1.0).abs() < 1e-12);
        assert!((eval_str("log(8,64)", &vars) - 2.0).abs() < 1e-12);
    }

    #[test]
    fn if_takes_only_the_chosen_branch() {
        let vars = Vars::new(&[("x", 5.0)]);
        assert_eq!(eval_str("if(x>0, 100, 200)", &vars), 100.0);
        assert_eq!(eval_str("if(x>9, 100, 200)", &vars), 200.0);
        // The untaken branch is not evaluated, so an unknown variable
        // there does not fail the call.
        assert_eq!(eval_str("if(x>0, 1, missing)", &vars), 1.0);
    }

    #[test]
    fn unknown_variable_is_reported() {
        let vars = Vars::new(&[]);
        let expr = Expression::parse("a + b").expect("parses");
        assert_eq!(
            expr.evaluate(&vars),
            Err(ExpressionError::UnknownVariable("a".to_string()))
        );
    }

    #[test]
    fn parse_errors() {
        assert!(matches!(
            Expression::parse("1 +"),
            Err(ExpressionError::UnexpectedEnd)
        ));
        assert!(matches!(
            Expression::parse("1 2"),
            Err(ExpressionError::UnexpectedToken(_))
        ));
        assert!(matches!(
            Expression::parse("nosuchfn(1)"),
            Err(ExpressionError::UnknownFunction(_))
        ));
        assert!(matches!(
            Expression::parse("pow(1)"),
            Err(ExpressionError::ArgCount { .. })
        ));
        assert!(matches!(
            Expression::parse("1 # 2"),
            Err(ExpressionError::UnexpectedChar('#'))
        ));
    }

    #[test]
    fn scientific_and_decimal_literals() {
        let vars = Vars::new(&[]);
        assert_eq!(eval_str("1.5e2", &vars), 150.0);
        assert_eq!(eval_str(".25 + .75", &vars), 1.0);
        assert_eq!(eval_str("2.0E-1 * 10", &vars), 2.0);
    }
}
