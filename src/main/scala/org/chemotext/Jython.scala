package org.chemotext

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.python.util.PythonInterpreter; 
import org.python.core._; 

case class Test ( var x : Int = 0 )

class X {
  var x : Int = 0
  def getX () = { x }
  def setX (v : Int) = { x = v }
}

class JythonInterpreter {

    val engine = new ScriptEngineManager().getEngineByName("python");

    // Using the eval() method on the engine causes a direct
    // interpretataion and execution of the code string passed into it
    engine.eval("import sys")
    engine.eval("print sys")

    // Using the put() method allows one to place values into
    // specified variables within the engine
    engine.put("a", "42")



  var t = new X () //Test ()
  engine.put ("t", t)
  engine.eval ("t.setX (9)")

  println (s" ---------- T: ${t.x} ")

  var m = engine.get ("t")
  println (s" ---------- M: ${t.x}")


    // As you can see, once the variable has been set with
    // a value by using the put() method, we an issue eval statements
    // to use it.
    engine.eval("print a")
    engine.eval("x = 2 + 2")

    // Using the get() method allows one to obtain the value
    // of a specified variable from the engine instance
    val x = engine.get("x")

}

