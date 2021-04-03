/*
 * Copyright 2013-2021 The Kamon Project <https://kamon.io>
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

package kamon
package context

import com.typesafe.config.Config
import kamon.tag.{Tag, TagSet}
import kamon.trace.{Span, SpanPropagation}
import org.slf4j.LoggerFactory

import scala.reflect.ClassTag
import scala.util.control.NonFatal

/**
  * Context propagation that uses HTTP headers as the transport medium. HTTP propagation mechanisms read any number of
  * HTTP headers from incoming HTTP requests to decode a Context instance and write any number of HTTP headers on
  * outgoing requests to transfer a context to remote services.
  */
object HttpPropagation {

  /**
    * Wrapper that reads HTTP headers from a HTTP message.
    */
  trait HeaderReader {

    /**
      * Reads a single HTTP header value.
      */
    def read(header: String): Option[String]

    /**
      * Returns a map with all HTTP headers present in the wrapped HTTP message.
      */
    def readAll(): Map[String, String]
  }

  /**
    * Wrapper that writes HTTP headers to a HTTP message.
    */
  trait HeaderWriter {

    /**
      * Writes a HTTP header into a HTTP message.
      */
    def write(header: String, value: String): Unit
  }

  /**
    * Create a new HTTP propagation instance from the provided configuration.
    */
  def from(propagationConfig: Config, identifierScheme: String): Propagation[HttpPropagation.HeaderReader, HttpPropagation.HeaderWriter] = {
    new HttpPropagation.Default(Settings.from(propagationConfig, identifierScheme))
  }

  /**
    * Default HTTP Propagation in Kamon.
    */
  class Default(settings: Settings) extends Propagation[HttpPropagation.HeaderReader, HttpPropagation.HeaderWriter] {

    private val log = LoggerFactory.getLogger(classOf[HttpPropagation.Default])

    /**
      * Reads a Context from a ReaderMedium instance. If there is any problem while reading the Context then Context.Empty
      * should be returned instead of throwing any exceptions.
      */
    override def read(reader: HeaderReader): Context =
      settings.incomingEntries.foldLeft(Context.Empty) {
        case (context, (entryName, entryDecoder)) =>
          var result = context
          try {
            result = entryDecoder.read(reader, context)
          } catch {
            case NonFatal(t) => log.warn("Failed to read entry [{}]", entryName.asInstanceOf[Any], t.asInstanceOf[Any])
          }

          result
      }

    /**
      * Attempts to write a Context instance to the WriterMedium.
      */
    override def write(context: Context, writer: HeaderWriter): Unit =
      settings.outgoingEntries.foreach {
        case (entryName, entryWriter) =>
          try {
            entryWriter.write(context, writer)
          } catch {
            case NonFatal(t) => log.warn("Failed to write entry [{}] due to: {}", entryName.asInstanceOf[Any], t.asInstanceOf[Any])
          }
      }

  }

  case class Settings(
    tagsHeaderName: String,
    includeUpstreamName: Boolean,
    tagsMappings: Map[String, String],
    incomingEntries: Map[String, Propagation.EntryReader[HeaderReader]],
    outgoingEntries: Map[String, Propagation.EntryWriter[HeaderWriter]]
  )

  object Settings {
    type ReaderWriter = Propagation.EntryReader[HeaderReader] with Propagation.EntryWriter[HeaderWriter]

    private val log = LoggerFactory.getLogger(classOf[HttpPropagation.Settings])
    private val readerWriterShortcuts: Map[String, ReaderWriter] = Map(
      "context/kamon" -> ContextPropagation.Kamon(),
      "context/w3c" -> ContextPropagation.W3CBaggage(),
      "span/b3" -> SpanPropagation.B3(),
      "span/uber" -> SpanPropagation.Uber(),
      "span/b3-single" -> SpanPropagation.B3Single(),
      "span/w3c" -> SpanPropagation.W3CTraceContext()
    )

    def from(config: Config, identifierScheme: String): Settings = {
      def buildInstances[ExpectedType : ClassTag](mappings: Map[String, String]): Map[String, ExpectedType] = {
        val instanceMap = Map.newBuilder[String, ExpectedType]

        mappings.foreach {
          case (contextKey, componentClass) =>
            val shortcut = s"$contextKey/$componentClass".toLowerCase()

            if (componentClass == "span/w3c" && identifierScheme != "double") {
              log.warn("W3C TraceContext propagation should be used only with identifier-scheme = double")
            }

            readerWriterShortcuts.get(shortcut).fold({
              try {
                instanceMap += (contextKey -> ClassLoading.createInstance[ExpectedType](componentClass, Nil))
              } catch {
                case exception: Exception => log.warn("Failed to instantiate {} [{}] due to []",
                  implicitly[ClassTag[ExpectedType]].runtimeClass.getName, componentClass, exception)
              }
            })(readerWriter => instanceMap += (contextKey -> readerWriter.asInstanceOf[ExpectedType]))
        }

        instanceMap.result()
      }

      val tagsHeaderName = config.getString("tags.header-name")
      val tagsIncludeUpstreamName = config.getBoolean("tags.include-upstream-name")
      val tagsMappings = config.getConfig("tags.mappings").pairs
      val incomingEntries = buildInstances[Propagation.EntryReader[HeaderReader]](config.getConfig("entries.incoming").pairs)
      val outgoingEntries = buildInstances[Propagation.EntryWriter[HeaderWriter]](config.getConfig("entries.outgoing").pairs)

      Settings(tagsHeaderName, tagsIncludeUpstreamName, tagsMappings, incomingEntries, outgoingEntries)
    }
  }

}
