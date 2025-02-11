<?php
/**
 * Class: WordPress Media Loader
 *
 * Handles loading media files into WordPress during ETL process.
 *
 * @package TenupETL\Classes\Load
 */

namespace TenupETL\Classes\Load\Loaders;

use TenupETL\Utils\WithLogging;

use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\ETL\Filesystem\SaveMode;

/**
 * Class WordPressMediaLoader
 *
 * Handles loading media files into WordPress during ETL process.
 */
class WordPressMediaLoader extends BaseLoader implements Loader {
	use WithLogging;
	use WithLedger;

	/**
	 * Run the loader
	 *
	 * @param object $state The ETL state object.
	 * @return void
	 */
	public function run( $state ) {
		$this->log( 'Loading Media into WordPress...', 'progress' );
		$state
			->mode( SaveMode::Overwrite )
			->write(
				$this
			);
	}

	/**
	 * Load rows into WordPress as media attachments
	 *
	 * @param Rows        $rows    The rows to load.
	 * @param FlowContext $context The flow context.
	 * @return void
	 */
	public function load( Rows $rows, FlowContext $context ): void {
		require_once ABSPATH . 'wp-admin/includes/file.php';
		require_once ABSPATH . 'wp-admin/includes/media.php';
		require_once ABSPATH . 'wp-admin/includes/image.php';

		foreach ( $rows as $row ) {
			$media = $this->reduce_row_on_prefix( $row, 'media' );

			$remote_urls = $media['remote_urls'];

			foreach ( $remote_urls as $remote_url ) {
				$attachment_id = $this->sideload_media( $remote_url );

				$this->create_ledger_entry(
					[
						'uid'           => $row->valueOf( 'etl.uid' ),
						'remote_url'    => $remote_url,
						'attachment_id' => $attachment_id,
					]
				);
			}
		}
	}

	/**
	 * Sideload a media file into WordPress
	 *
	 * @param string $file_url URL of the file to sideload.
	 * @param int    $post_id  Optional. Post ID to attach the media to.
	 * @return int|WP_Error Attachment ID on success, WP_Error on failure.
	 */
	protected function sideload_media( $file_url, $post_id = 0 ) {
		// Download the file to a temporary location
		$temp_file = download_url( $file_url );

		if ( is_wp_error( $temp_file ) ) {
			return $temp_file; // Return error if download failed
		}

		// Set up file array
		$file = [
			'name'     => basename( $file_url ),
			'tmp_name' => $temp_file,
		];

		// Upload the file and attach it to the post
		$attachment_id = media_handle_sideload( $file, $post_id );

		// Check for errors
		if ( is_wp_error( $attachment_id ) ) {
			wp_delete_file( $temp_file ); // Cleanup temporary file
			return $attachment_id;
		}

		return $attachment_id;
	}
}
