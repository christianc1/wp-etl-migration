<?php
/**
 * Class: WordPress Post Loader
 *
 * Loads posts into WordPress from ETL rows.
 *
 * @package TenupETL\Classes\Load\Loaders
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\{ WithLogging, WithSideLoadMedia };

use Flow\ETL\{FlowContext, Loader, Rows};

/**
 * Class WordPressPostLoader
 *
 * Handles loading posts into WordPress during ETL process.
 */
class WordPressPostLoader extends BaseLoader implements Loader {
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
		$state
			->write(
				$this
			);

		$this->log( 'Loading posts with WordPress core apis.', 'progress' );
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
			$post_arr  = $this->reduce_row_on_prefix( $row, 'post' );
			$post_meta = $this->reduce_row_on_prefix( $row, 'meta' );
			$tax_terms = $this->reduce_row_on_prefix( $row, 'tax' );
			$ledger    = $this->reduce_row_on_prefix( $row, 'ledger' );

			// Cast DateTime to String
			$post_arr = array_map(
				function ( $value ) {
					return $value instanceof \DateTimeInterface ? $value->format( 'Y-m-d H:i:s' ) : $value;
				},
				$post_arr
			);

			if ( $post_arr['post_content'] instanceof \DomDocument ) {
				$post_arr['post_content'] = $post_arr['post_content']->saveHTML();
			}

			$post_id = wp_insert_post( $post_arr );

			// Handle thumbnail
			if ( $post_meta['_remote_featured_media'] ) {
				$attachment_id = $this->sideload_media( $post_meta['_remote_featured_media'], $post_id );

				if ( ! is_wp_error( $attachment_id ) ) {
					set_post_thumbnail( $post_id, $attachment_id );
				}

				unset( $post_meta['_remote_featured_media'] );
			}

			// Handle the meta.
			foreach ( $post_meta as $meta_key => $meta_value ) {
				update_post_meta( $post_id, $meta_key, $meta_value );
			}

			// Handle the terms
			if ( ! empty( $tax_terms ) ) {
				foreach ( $tax_terms as $taxonomy => $term_slugs ) {
					wp_set_object_terms( $post_id, $term_slugs, $taxonomy, true );
				}
			}

			// Create a ledger entry
			$this->create_ledger_entry(
				array_merge(
					[
						'post_id' => $post_id,
						'uid'     => $row->valueOf( 'etl.uid' ),
					],
					$ledger
				)
			);
			do_action( 'tenup_etl_row_processed', 1 );
			$this->log( 'Loaded WP_Post ' . $post_id, 'progress' );
		}
	}
}
