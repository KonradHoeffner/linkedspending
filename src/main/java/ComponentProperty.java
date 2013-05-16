import com.hp.hpl.jena.rdf.model.Property;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@AllArgsConstructor	
@EqualsAndHashCode
@ToString
public class ComponentProperty
{
	public final Property property;
	public final String name;	
	public static enum Type {ATTRIBUTE,MEASURE,DATE, COMPOUND};
	public final Type type;
}