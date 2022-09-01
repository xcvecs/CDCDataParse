package top.byteinfo.source.maxwell.schema;


import top.byteinfo.iter.schema.columndef.ColumnDef;

import java.util.ArrayList;
import java.util.List;

public class CustomDatabase {
	private final String name;
	private final List<CustomTable> customTableList;
	private String charset;
	private CustomSchemaCapture.CaseSensitivity sensitivity;

	public CustomDatabase(String name, List<CustomTable> customTables, String charset) {
		this.name = name;
		if ( customTables == null )
			this.customTableList = new ArrayList<>();
		else
			this.customTableList = customTables;
		this.charset = charset;
	}

	public CustomDatabase(String name, String charset) {
		this(name, null, charset);
	}

	public List<String> getTableNames() {
		ArrayList<String> names = new ArrayList<String>();
		for ( CustomTable t : this.customTableList) {
			names.add(t.getName());
		}
		return names;
	}

	private boolean compareTableNames(String a, String b) {
		if ( sensitivity == CustomSchemaCapture.CaseSensitivity.CASE_SENSITIVE )
			return a.equals(b);
		else
			return a.toLowerCase().equals(b.toLowerCase());
	}

	public CustomTable findTable(String name) {
		for ( CustomTable t: this.customTableList) {
			if ( compareTableNames(name, t.getName()))
				return t;
		}
		return null;
	}

	public CustomTable findTableOrThrow(String table) throws Exception {
		CustomTable t = findTable(table);
		if ( t == null )
			throw new RuntimeException("Couldn't find table '" + table + "'" + " in database " + this.name);

		return t;
	}

	public boolean hasTable(String name) {
		return findTable(name) != null;
	}

	public void removeTable(String name) {
		CustomTable t = findTable(name);
		if ( t != null )
			customTableList.remove(t);
	}

	public CustomDatabase copy() {
		CustomDatabase d = new CustomDatabase(this.name, this.charset);
		for ( CustomTable t: this.customTableList) {
			d.addTable(t.copy());
		}
		return d;
	}

	private void diffTableList(List<String> diffs, CustomDatabase a, CustomDatabase b, String nameA, String nameB, boolean recurse) {
		for ( CustomTable t : a.getTableList() ) {
			CustomTable other = b.findTable(t.getName());
			if ( other == null )
				diffs.add("database " + a.getName() + " did not contain table " + t.getName() + " in " + nameB);
			else if ( recurse )
				t.diff(diffs, other, nameA, nameB);
		}
	}

	public void diff(List<String> diffs, CustomDatabase other, String nameA, String nameB) {
		if ( !this.charset.toLowerCase().equals(other.getCharset().toLowerCase()) ) {
			diffs.add("-- Database " + this.getName() + " had different charset: "
					+ this.getCharset() + " in " + nameA + ", "
					+ other.getCharset() + " in " + nameB);
		}
		diffTableList(diffs, this, other, nameA, nameB, true);
		diffTableList(diffs, other, this, nameB, nameA, false);
	}

	public String getCharset() {
		if ( charset == null ) {
			// TODO: return server-default charset
			return "";
		} else {
		    return charset;
		}
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public String getName() {
		return name;
	}

	public List<CustomTable> getTableList() {
		return customTableList;
	}

	public void addTable(CustomTable customTable) {
		customTable.setDatabase(this.name);
		this.customTableList.add(customTable);
	}

	public CustomTable buildTable(String name, String charset, List<ColumnDef> list, List<String> pks) {
		if ( charset == null )
			charset = getCharset(); // inherit database's default charset

		if ( sensitivity == CustomSchemaCapture.CaseSensitivity.CONVERT_TO_LOWER )
			name = name.toLowerCase();

		CustomTable t = new CustomTable(this.name, name, charset, list, pks);

		this.customTableList.add(t);
		return t;
	}

	public CustomTable buildTable(String name, String charset) {
		return buildTable(name, charset, new ArrayList<ColumnDef>(), null);
	}

	public void setSensitivity(CustomSchemaCapture.CaseSensitivity sensitivity) {
		this.sensitivity = sensitivity;
	}
}
