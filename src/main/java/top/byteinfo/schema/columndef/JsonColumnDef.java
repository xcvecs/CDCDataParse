package top.byteinfo.schema.columndef;

import com.github.shyiko.mysql.binlog.event.deserialization.json.JsonBinary;

import java.io.IOException;

public class JsonColumnDef extends ColumnDef {
	private JsonColumnDef(String name, String type, short pos) {
		super(name, type, pos);
	}

	public static JsonColumnDef create(String name, String type, short pos) {
		JsonColumnDef temp = new JsonColumnDef(name, type, pos);
		return (JsonColumnDef) INTERNER.intern(temp);
	}

	@Override
	public Object asJSON(Object value, MaxwellOutputConfig config) throws ColumnDefCastException {
		String jsonString;

		if ( value instanceof String ) {
			return new RawJSONString((String) value);
		} else if ( value instanceof byte[] ){
			try {
				byte[] bytes = (byte[]) value;
				jsonString = bytes.length > 0 ? JsonBinary.parseAsString(bytes) : "null";
				return new RawJSONString(jsonString);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new ColumnDefCastException(this, value);
		}
	}

	@Override
	public String toSQL(Object value) {
		return null;
	}
}
