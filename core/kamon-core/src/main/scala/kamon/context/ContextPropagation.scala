package kamon.context

import kamon.context.ContextPropagation.Kamon.Settings
import kamon.context.HttpPropagation.{HeaderReader, HeaderWriter}
import kamon.tag.{Tag, TagSet}
import kamon.trace.Span
import org.slf4j.LoggerFactory

import scala.util.control.NonFatal

object ContextPropagation {

  /**
    * Default HTTP context propagation in Kamon.
    */
  class Kamon(settings: Settings) extends Propagation[HttpPropagation.HeaderReader, HttpPropagation.HeaderWriter] {

    private val log = LoggerFactory.getLogger(classOf[ContextPropagation.Kamon])

    /**
      * Reads context tags and entries on the following order:
      *   1. Read all context tags from the context tags header.
      *   2. Read all context tags with explicit mappings. This overrides any tag
      *      from the previous step in case of a tag key clash.
      *   3. Read all context entries using the incoming entries configuration.
      */
    override def read(reader: HeaderReader): Context = {
      val tags = Map.newBuilder[String, Any]

      // Tags explicitly mapped on the tags.mappings configuration.
      settings.tagsMappings.foreach {
        case (tagName, httpHeader) =>
          try {
            reader.read(httpHeader).foreach(tagValue => tags += (tagName -> tagValue))
          } catch {
            case NonFatal(t) => log.warn("Failed to read mapped tag [{}]", tagName, t.asInstanceOf[Any])
          }
      }

      // Tags encoded together in the context tags header.
      try {
        reader.read(settings.tagsHeaderName).foreach { contextTagsHeader =>
          contextTagsHeader.split(";").foreach(tagData => {
            val tagPair = tagData.split("=")
            if (tagPair.length == 2) {
              tags += (tagPair(0) -> parseTagValue(tagPair(1)))
            }
          })
        }
      } catch {
        case NonFatal(t) => log.warn("Failed to read the context tags header", t.asInstanceOf[Any])
      }

      Context.of(TagSet.from(tags.result()))
    }

    /**
      * Writes context tags and entries
      */
    override def write(context: Context, writer: HeaderWriter): Unit = {
      val contextTagsHeader = new StringBuilder()
      def appendTag(key: String, value: String): Unit = {
        contextTagsHeader
          .append(key)
          .append('=')
          .append(value)
          .append(';')
      }

      // Write tags with specific mappings or append them to the context tags header.
      context.tags.iterator().foreach { tag =>
        val tagKey = tag.key

        settings.tagsMappings.get(tagKey) match {
          case Some(mappedHeader) => writer.write(mappedHeader, tagValueWithPrefix(tag))
          case None               => appendTag(tagKey, Tag.unwrapValue(tag).toString)
        }
      }

      // Write the upstream name, if needed
      if(settings.includeUpstreamName)
        appendTag(Span.TagKeys.UpstreamName, Kamon.environment.service)

      // Write the context tags header.
      if(contextTagsHeader.nonEmpty) {
        writer.write(settings.tagsHeaderName, contextTagsHeader.result())
      }
    }

    private val _longTypePrefix = "l:"
    private val _booleanTypePrefix = "b:"

    /**
      * Tries to infer and parse a value into one of the supported tag types: String, Long or Boolean by looking for the
      * type indicator prefix on the tag value. If the inference fails it will default to treat the value as a String.
      */
    private def parseTagValue(value: String): Any = {
      if (value.length < 2) // Empty and short values definitely do not have type indicators.
        value
      else {
        if(value.startsWith(_longTypePrefix)) {
          // Try to parse the content as a Long value.
          val remaining = value.substring(2)
          try {
            remaining.toLong
          } catch {
            case _: java.lang.NumberFormatException => remaining
          }

        } else if(value.startsWith(_booleanTypePrefix)) {
          // Try to parse the content as a Boolean value.
          val remaining = value.substring(2)
          if(remaining == "true")
            true
          else if(remaining == "false")
            false
          else
            remaining

        } else value
      }
    }

    /**
      * Returns the actual value to be written in the HTTP transport, with a type prefix if applicable.
      */
    private def tagValueWithPrefix(tag: Tag): String = tag match {
      case t: Tag.String  => t.value
      case t: Tag.Boolean => _booleanTypePrefix + t.value.toString
      case t: Tag.Long    => _longTypePrefix + t.value.toString
    }

  }

  object Kamon {

    case class Settings(
      tagsHeaderName: String,
      includeUpstreamName: Boolean,
      tagsMappings: Map[String, String]
    )

    def apply(settings: Settings): Kamon =
      new Kamon(settings)

  }

  class W3CBaggage extends Propagation[HttpPropagation.HeaderReader, HttpPropagation.HeaderWriter] {

    import W3CBaggage._

    private val log = LoggerFactory.getLogger(classOf[ContextPropagation.W3CBaggage)

    override def read(reader: HeaderReader): Context = {
      val tags = Map.newBuilder[String, Any]

      try {
        reader.read(HeaderName).foreach { contextTagsHeader =>
          contextTagsHeader.split(",").foreach(tagData => {
            val tagPair = tagData.split("=")
            if (tagPair.length == 2) {
              tags += (tagPair(0).trim -> tagPair(1).trim)
            }
          })
        }
      } catch {
        case NonFatal(t) => log.warn("Failed to read the context tags header", t.asInstanceOf[Any])
      }

      Context.of(TagSet.from(tags.result()))
    }

    override def write(context: Context, writer: HeaderWriter): Unit = {
      val contextTagsHeader = new StringBuilder()
      def appendTag(key: String, value: String): Unit = {
        contextTagsHeader
          .append(key)
          .append('=')
          .append(value)
          .append(',')
      }
    }

  }

  object W3CBaggage {

    val HeaderName = "baggage"

    def apply(): W3CBaggage =
      new W3CBaggage()

  }

}
