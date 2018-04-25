package com.linkedin.pinot.controller.api;

import com.fasterxml.jackson.databind.JavaType;
import io.swagger.converter.ModelConverter;
import io.swagger.converter.ModelConverterContext;
import io.swagger.models.Model;
import io.swagger.models.properties.Property;
import io.swagger.models.properties.StringProperty;
import io.swagger.util.Json;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Iterator;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

/**
 * A converter used to suppress the example displayed in swagger UI.
 */
public class FormDataMultiPartConverter implements ModelConverter {

  @Override
  public Property resolveProperty(Type type, ModelConverterContext context, Annotation[] annotations, Iterator<ModelConverter> chain) {
    JavaType jtype = Json.mapper().constructType(type);
    if (jtype != null) {
      Class<?> cls = jtype.getRawClass();
      if(FormDataMultiPart.class.isAssignableFrom(cls)) {
        return new StringProperty();
      }
    }
    if (chain.hasNext()) {
      return chain.next().resolveProperty(type, context, annotations, chain);
    } else {
      return null;
    }
  }

  @Override
  public Model resolve(Type type, ModelConverterContext context, Iterator<ModelConverter> chain) {
    if (chain.hasNext()) {
      return chain.next().resolve(type, context, chain);
    } else {
      return null;
    }
  }
}
