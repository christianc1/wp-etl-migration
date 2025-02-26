<?php

declare(strict_types=1);

namespace TenupETL\Classes\Transform\Transformers;

use function Flow\ETL\DSL\{row, rows, str_entry};
use Flow\ETL\Row\{Reference, References};
use Flow\ETL\{FlowContext, Rows, Row, Transformer};

final class SelectPrefixTransformer implements Transformer {
   /**
	 * Constructor.
	 *
	 * @param string|string[] $prefix The prefix to add to entry names.
	 */
	public function __construct(
		protected string | array $prefix,
		protected readonly bool $remove_prefix = false )
	{
		$this->prefix = is_array( $prefix ) ? $prefix : [ $prefix ];
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
				$names = [];
				foreach ( $row->entries()->all() as $entry ) {
					$names[] = $entry->name();
				}

				// Remove any entries that don't start with the prefix.
				$names = array_filter(
					$names,
					function($name) {
						foreach ($this->prefix as $prefix) {
							if (strpos($name, $prefix) === 0) {
								return false;
							}
						}
						return true;
					}
				);

				$row   = $row->remove( ...$names );

				// Remove the prefix from the entries, if $remove_prefix is true.
				if ( $this->remove_prefix ) {
					foreach ( $row->entries()->all() as $entry ) {
						foreach ( $this->prefix as $prefix ) {
							if ( strpos( $entry->name(), $prefix ) === 0 ) {
								$row = $row->rename( $entry->name(), str_replace( $prefix, '', $entry->name() ) );
							}
						}
					}
				}

				return $row;
			}
		);
	}
}
