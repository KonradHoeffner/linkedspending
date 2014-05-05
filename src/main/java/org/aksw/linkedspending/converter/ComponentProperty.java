package org.aksw.linkedspending.converter;
import com.hp.hpl.jena.rdf.model.Property;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

/**RDF Data Cube Component Property.**/
@AllArgsConstructor
@EqualsAndHashCode
@ToString
public class ComponentProperty
{
    @NonNull public final Property property;
    @NonNull public final String name;
    public static enum Type {ATTRIBUTE,MEASURE,DATE, COMPOUND};
    @NonNull public final Type type;
}