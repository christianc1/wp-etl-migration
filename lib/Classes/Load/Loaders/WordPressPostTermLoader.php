<?php
/**
 * Class: WordPress Term Loader
 *
 * Loads terms into WordPress from ETL rows.
 *
 * @package TenupETL\Classes\Load\Loaders
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\{ WithLogging, WithSideLoadMedia };

use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Row\EntryFactory;

use function Flow\ETL\DSL\{data_frame, from_array, lit, schema, json_schema, uuid_schema};

/**
 * Class WordPressTermLoader
 *
 * Handles loading terms into WordPress during ETL process.
 */
class WordPressTermLoader extends BaseLoader implements Loader {
	use WithLogging;
	use WithSideLoadMedia;
	use WithLedger;

	/**
	 * Run the loader
	 *
	 * @param object $state The ETL state object.
	 * @return void
	 */
	public function run( $state ) {
		$this->state = $state;
		$state
			->write(
				$this
			);

		$this->log( 'Loading terms with WordPress core apis.', 'progress' );
	}

	/**
	 * Load rows into WordPress as posts
	 *
	 * @param Rows        $rows    The rows to load.
	 * @param FlowContext $context The flow context.
	 * @return void
	 */
	public function load( Rows $rows, FlowContext $context ): void {

		foreach ( $rows as $row ) {
			$tax_terms_ids = [];
			$tax_terms = $this->reduce_row_on_prefix( $row, 'tax' );

			if ( ! $tax_terms ) {
				continue;
			}

			foreach ( $tax_terms as $taxonomy => $term_names ) {

				foreach ( $term_names as $term_name ) {

					$existing_term = term_exists( $term_name, $taxonomy );

					if ( $existing_term && $existing_term['term_id'] ) {
						continue;
					}

					$new_term = wp_insert_term( $term_name, $taxonomy, [ 'slug' => $term_name ] );

					if ( is_wp_error( $new_term ) ) {
						$this->log( 'Error inserting term: ' . $new_term->get_error_message(), 'error' );
						continue;
					}

					$tax_terms_ids[ $taxonomy ][] = $new_term['term_id'];

					$this->create_ledger_entry(
						[
							'uid'          => $row->valueOf( 'etl.uid' ),
							'taxonomy'     => $taxonomy,
							'term_name'    => $term_name,
							'term_id'      => $new_term['term_id'],
						],
					);
				}
			}

			do_action( 'tenup_etl_row_processed', 1 );
			$this->log( 'Loaded terms for row: ' . $row->valueOf( 'etl.uid' ), 'progress' );
		}
	}
}
