<?php
/**
 * WithSideLoadMedia Trait
 *
 * @package TenupETL\Utils
 */

namespace TenupETL\Utils;

trait WithSideLoadMedia {

	/**
	 * Sideloads media from a URL into the WordPress media library
	 *
	 * @param string $file_url The URL of the file to sideload
	 * @param int    $post_id  Optional. The post ID to attach the media to
	 * @return int|WP_Error    Attachment ID on success, WP_Error on failure
	 */
	protected function sideload_media( $file_url, $post_id = 0 ) {
		$filename = basename( $file_url );

		// Check if file already exists in media library
		$existing_attachment = get_posts(
			array(
				'post_type'      => 'attachment',
				'posts_per_page' => 1,
				'title'          => $filename,
				'fields'         => 'ids',
			)
		);

		if ( ! empty( $existing_attachment ) ) {
			return $existing_attachment[0];
		}

		require_once ABSPATH . 'wp-admin/includes/file.php';
		require_once ABSPATH . 'wp-admin/includes/media.php';
		require_once ABSPATH . 'wp-admin/includes/image.php';

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
		return media_handle_sideload( $file, $post_id );
	}
}
