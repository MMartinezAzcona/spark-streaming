package part1recap

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App{

  // values & variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if(2>3) "Bigger" else "Smaller"

  // function
  def myFunction(x: Int) = 42

  // OOP
  class Animal
  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton

  // companions
  object Carnivore

  //generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming
  val incrementer: Int => Int = x => x +1
  val incremented = incrementer(42)

  val processedList = List(1, 2, 3).map(incrementer)

  // Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // Future --> Abstract the way of computation in different threads
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    // some expensive computation which runs on another thread
    42
  }
  aFuture.onComplete {
    case Success(meaningOfLife) => println(s"Found $meaningOfLife")
    case Failure(ex) => println(s"Failed: $ex")
  }

  // Partial functions
  val aPartialFunction = (x: Int) => x match {
    case 1 => 50
    case 0 => 75
    case _ => 1000
  }
  val aPartialFunc: PartialFunction[Int, Int] = {
    case 1 => 50
    case 0 => 75
    case _ => 1000
  }
  println(aPartialFunc(0))

  // Implicits
  def methodWithImplicitArg(implicit x: Int) = x+40
  implicit val ImplicitInt = 67
  println(methodWithImplicitArg)

  // Implicit conversions --> implicit defs
  case class Person(name: String) {
    def greet: Unit = println(s"Hi, my name is $name")
  }

  implicit def fromStringToPerson(name: String): Person = Person(name)
  "Bob".greet //The compiler automatically converts the string "Bob" to Person an allows using its methods

  // Implicit conversions --> implicit classes (Preferable to implicit devs)
  implicit class Dog(name: String) {
    def bark: Unit = println("Bark! Bark!")
  }
  "Lassie".bark

  /** How the compiler recognize which implicit to consider?
    * -local scope
    * -imported scope
    * -companion objects of the types involved in the call
    * */

}
