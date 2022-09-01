package top.byteinfo.iter.schema.columndef;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SetColumnDef extends EnumeratedColumnDef {
	private SetColumnDef(String name, String type, short pos, String[] enumValues) {
		super(name, type, pos, enumValues);
	}

	public static SetColumnDef create(String name, String type, short pos, String[] enumValues) {
		SetColumnDef temp = new SetColumnDef(name, type, pos, enumValues);
		return (SetColumnDef) INTERNER.intern(temp);
	}

	@Override
	public String toSQL(Object value) throws ColumnDefCastException {
		return "'" + StringUtils.join(asList(value), "'") + "'";
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		return asList(value);
	}

	private ArrayList<String> asList(Object value) throws ColumnDefCastException {
		if ( value instanceof String ) {
			return new ArrayList<>(Arrays.asList((( String ) value).split(",")));
		} else if ( value instanceof Long ) {
			ArrayList<String> values = new ArrayList<>();
			long v = (Long) value;
			List<String> enumValues = getEnumValues();
			for (int i = 0; i < enumValues.size(); i++) {
				if (((v >> i) & 1) == 1) {
					values.add(enumValues.get(i));
				}
			}
			return values;
		} else {
			throw new ColumnDefCastException(this, value);
		}
	}
}
