import com.hp.hpl.jena.rdf.model.Property;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

@AllArgsConstructor	
@EqualsAndHashCode
@ToString
public class ComponentProperty
{
	@NonNull public final Property property;
	@NonNull public final String name;	
	public static enum Type {ATTRIBUTE,MEASURE,DATE, COMPOUND};
	@NonNull public final Type type;
//	@Override public boolean equals(Object o)
//	{
//		if (o == null)
//			  return false;
//			if (!(o instanceof ComponentProperty))
//			  return false;
//	...
//	}
}