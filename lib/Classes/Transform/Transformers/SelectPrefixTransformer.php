<?php

declare(strict_types=1);

namespace TenupETL\Classes\Transform\Transformers;

use function Flow\ETL\DSL\{row, rows, str_entry};
use Flow\ETL\Row\{Reference, References};
use Flow\ETL\{FlowContext, Rows, Row, Transformer};

final readonly class SelectPrefixTransformer implements Transformer {
   /**
	 * Constructor.
	 *
	 * @param string $prefix The prefix to add to entry names.
	 */
	public function __construct( private readonly string $prefix, private readonly bool $remove_prefix = false ) {}

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

				// Remove any entries that start with the prefix.
				$names = array_filter( $names, fn( $name ) => ! ( strpos( $name, $this->prefix ) === 0 ) );
				$row   = $row->remove( ...$names );

				// Remove the prefix from the entries, if $remove_prefix is true.
				if ( $this->remove_prefix ) {
					foreach ( $row->entries()->all() as $entry ) {
						$row = $row->rename( $entry->name(), str_replace( $this->prefix, '', $entry->name() ) );
					}
				}

				return $row;
			}
		);
	}
}
