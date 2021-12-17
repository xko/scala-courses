package calculator

enum Expr:
  case Literal(v: Double)
  case Ref(name: String)
  case Plus(a: Expr, b: Expr)
  case Minus(a: Expr, b: Expr)
  case Times(a: Expr, b: Expr)
  case Divide(a: Expr, b: Expr)

object Calculator extends CalculatorInterface:
  import Expr.*

  def computeValues(namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] =
    for (name,sig)<- namedExpressions yield
      (name, Signal( eval(sig(),namedExpressions - name) ))

  def eval(expr: Expr, references: Map[String, Signal[Expr]])(using Signal.Caller): Double =
    expr match
      case Literal(v) => v
      case Plus(a,b) => eval(a, references)+eval(b, references)
      case Minus(a,b) => eval(a, references)-eval(b, references)
      case Times(a,b) => eval(a, references)*eval(b, references)
      case Divide(a,b) => eval(a, references)/eval(b, references)
      case Ref(name) if references.contains(name) => eval(references(name)(), references)
      case _ => Double.NaN


  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]])(using Signal.Caller): Expr =
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
