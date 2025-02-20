<?php
/**
 * Transformer: RenameWithPrefixAllEntriesTransformer
 *
 * Renames all entries in a row by adding a prefix to their names.
 *
 * @package TenupETL\Classes\Transform\Transformers
 */

namespace TenupETL\Classes\Transform\Transformers;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\{FlowContext, Rows, Row, Transformer};
use Flow\ETL\Row\EntryFactory;
/**
 * Transformer that adds a prefix to all entry names.
 */
final class StringToNullTransformer implements Transformer {

	/**
	 * Constructor.
	 *
	 * @param string $prefix The prefix to add to entry names.
	 */
	public function __construct( private readonly string $string ) {
		$this->entry_factory = new EntryFactory();
	}

	/**
	 * Transforms rows by adding a prefix to all entry names.
	 *
	 * @param Rows        $rows    The rows to transform.
	 * @param FlowContext $context The flow context.
	 * @return Rows The transformed rows.
	 */
	public function transform( Rows $rows, FlowContext $context ): Rows {
		return $rows->map(
			function ( Row $row ): Row {
				foreach ( $row->entries()->all() as $entry ) {
					if ( $entry->value() === $this->string ) {
						$row = $row->set( $this->entry_factory->create( $entry->name(), null ) );
					}
				}

				return $row;
			}
		);
	}
}
