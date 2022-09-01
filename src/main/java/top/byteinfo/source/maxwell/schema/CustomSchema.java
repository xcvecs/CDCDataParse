package top.byteinfo.source.maxwell.schema;

import org.apache.commons.lang3.tuple.Pair;
import top.byteinfo.iter.schema.columndef.ColumnDef;

import java.util.*;


public class CustomSchema {
	private final LinkedHashMap<String, CustomDatabase> dbMap;
	private final String charset;
	private final CustomSchemaCapture.CaseSensitivity sensitivity;

	public CustomSchema(List<CustomDatabase> customDatabases, String charset, CustomSchemaCapture.CaseSensitivity sensitivity) {
		this.sensitivity = sensitivity;
		this.charset = charset;
		this.dbMap = new LinkedHashMap<>();

		for ( CustomDatabase d : customDatabases)
			addDatabase(d);
	}

	public Collection<CustomDatabase> getDatabases() { return Collections.unmodifiableCollection(this.dbMap.values()); }

	public List<String> getDatabaseNames () {
		ArrayList<String> names = new ArrayList<>(this.dbMap.size());

		for ( CustomDatabase d : this.dbMap.values() ) {
			names.add(d.getName());
		}
		return names;
	}

	public CustomDatabase findDatabase(String string) {
		return this.dbMap.get(getNormalizedDbName(string));
	}

	private String getNormalizedDbName(String dbName) {
		if (dbName == null) {
			return null;
		}
		if (sensitivity == CustomSchemaCapture.CaseSensitivity.CASE_SENSITIVE) {
			return dbName;
		} else {
			return dbName.toLowerCase();
		}
	}

	public CustomDatabase findDatabaseOrThrow(String name) throws Exception {
		CustomDatabase d = findDatabase(name);
		if ( d == null )
			throw new RuntimeException("Couldn't find database '" + name + "'");
		return d;
	}

	public boolean hasDatabase(String string) {
		return findDatabase(string) != null;
	}

	public void addDatabase(CustomDatabase d) {
		d.setSensitivity(sensitivity);
		this.dbMap.put(getNormalizedDbName(d.getName()), d);
	}

	public void removeDatabase(CustomDatabase d) {
		this.dbMap.remove(getNormalizedDbName(d.getName()));
	}

	private void diffDBList(List<String> diff, CustomSchema a, CustomSchema b, String nameA, String nameB, boolean recurse) {
		for ( CustomDatabase d : a.dbMap.values() ) {
			CustomDatabase matchingDB = b.findDatabase(d.getName());

			if ( matchingDB == null )
				diff.add("-- Database " + d.getName() + " did not exist in " + nameB);
			else if ( recurse )
				d.diff(diff, matchingDB, nameA, nameB);
		}

	}

	public List<String> diff(CustomSchema that, String thisName, String thatName) {
		List<String> diff = new ArrayList<>();

		diffDBList(diff, this, that, thisName, thatName, true);
		diffDBList(diff, that, this, thatName, thisName, false);
		return diff;
	}

	public boolean equals(CustomSchema that) {
		return diff(that, "a", "b").size() == 0;
	}

	public String getCharset() {
		return charset;
	}

	public CustomSchemaCapture.CaseSensitivity getCaseSensitivity() {
		return sensitivity;
	};

	public List<Pair<FullColumnDef, FullColumnDef>> matchColumns(CustomSchema thatCustomSchema) {
		ArrayList<Pair<FullColumnDef, FullColumnDef>> list = new ArrayList<>();

		for ( CustomDatabase thisCustomDatabase : this.getDatabases() ) {
			CustomDatabase thatCustomDatabase = thatCustomSchema.findDatabase(thisCustomDatabase.getName());

			if ( thatCustomDatabase == null )
				continue;

			for ( CustomTable thisCustomTable : thisCustomDatabase.getTableList() ) {
				CustomTable thatCustomTable = thatCustomDatabase.findTable(thisCustomTable.getName());

				if ( thatCustomTable == null )
					continue;

				for ( ColumnDef thisColumn : thisCustomTable.getColumnList() ) {
					ColumnDef thatColumn = thatCustomTable.findColumn(thisColumn.getName());
					if ( thatColumn != null )
						list.add(Pair.of(
								new FullColumnDef(thisCustomDatabase, thisCustomTable, thisColumn),
								new FullColumnDef(thatCustomDatabase, thatCustomTable, thatColumn)
						));
				}
			}
		}
		return list;
	}


	public static class FullColumnDef {
		private final CustomDatabase db;
		private final CustomTable customTable;
		private final ColumnDef columnDef;

		public FullColumnDef(CustomDatabase db, CustomTable customTable, ColumnDef columnDef) {
			this.db = db;
			this.customTable = customTable;
			this.columnDef = columnDef;
		}

		public CustomDatabase getDb() {
			return db;
		}

		public CustomTable getTable() {
			return customTable;
		}

		public ColumnDef getColumnDef() {
			return columnDef;
		}
	}
}
