package org.apache.iceberg;

import org.apache.iceberg.mapping.NameMapping;

/**
 * API for updating name mapping.
 */
public interface UpdateNameMapping extends PendingUpdate<NameMapping> {
  /**
   * Add a set of aliases to an existing column.
   *
   * @param name    name of the column for which aliases will be added to
   * @param aliases set of aliases that need to be added to the column
   * @return this for method chaining
   */
  UpdateNameMapping addAliases(String name, Iterable<String> aliases);

  /**
   * Add a set of aliases to a nested struct.
   * <p>
   * The parent name is used to find the parent using {@link Schema#findField(String)}. If parent
   * identifies itself as a struct and contains the column name as a nested field, then aliases
   * are added for this field. If it identifies as a list, the aliases are added to the list element struct, and if
   * it identifies as a map, the aliases are added to the map's value struct.
   * </p>
   *
   * @param parent  name of the parent struct
   * @param name    name of the column for which the aliases will be added to
   * @param aliases set of aliases for the new column
   * @return this for method chaining
   * @throws IllegalArgumentException If parent doesn't identify a struct
   */
  UpdateNameMapping addAliases(
      String parent, String name, Iterable<String> aliases) throws IllegalArgumentException;
}
