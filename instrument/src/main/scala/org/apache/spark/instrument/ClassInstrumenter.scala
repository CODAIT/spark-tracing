/* Copyright 2017 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.instrument

import java.io.ByteArrayInputStream
import java.lang.instrument.ClassFileTransformer
import java.security.ProtectionDomain
import javassist._
import org.apache.spark.instrument.tracers._
import com.typesafe.config._

class ClassInstrumenter() extends ClassFileTransformer {
  private def getTracer(pkg: String, target: Config): Tracer = {
    target.getString("type") match {
      case "event" => new Event(pkg + "." + target.getString("class"), target.getString("method"))
      case "span" => new Span(pkg + "." + target.getString("class"), target.getString("method"))
      case "local" => new Local(pkg + "." + target.getString("class"), target.getString("method"), target.getString("variable"))
      case "rpc" => new RpcLogger
      case x => throw new RuntimeException(s"Unknown tracer type $x")
    }
  }

  val packages: Set[String] = Config.targets().keys.map(_.replace(".", "/")).toSet

  val targets: Seq[Tracer] = Seq(new MainLogger) ++ Config.targets().flatMap(pkg => {
    pkg._2.map(target => getTracer(pkg._1, target))
  })

  def instrumentClass(cls: CtClass): CtClass = {
    cls.getDeclaredBehaviors.foreach(method => {
      targets.filter(_.matches(method)).foreach(target => {
        //println(s"Instrumenting ${method.getLongName} with ${target.toString}")
        try {
          if ((method.getModifiers & Modifier.ABSTRACT) != 0)
            throw new RuntimeException(s"Cannot instrument abstract method ${method.getLongName}")
          target.apply(method)
        }
        catch { case e: Throwable => e.printStackTrace() }
      })}
    )
    cls
  }

  override def transform(loader: ClassLoader, name: String, curClass: Class[_], protectionDomain: ProtectionDomain,
    buffer: Array[Byte]): Array[Byte] = {
    TraceWriter.runAsOverhead {
      val ret = if (packages.exists(x => name.startsWith(x))) {
        val targetClass = ClassPool.getDefault.makeClass(new ByteArrayInputStream(buffer))
        instrumentClass(targetClass).toBytecode()
      }
      else null
      ret
    }
  }
}
