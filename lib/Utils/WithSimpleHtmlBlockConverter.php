<?php
/**
 * WithSimpleHtmlBlockConverter Trait
 *
 * @package TenupETL\Utils
 */

namespace TenupETL\Utils;

use Symfony\Component\DomCrawler\Crawler;

trait WithSimpleHtmlBlockConverter {

	/**
	 * Converts HTML content to Gutenberg blocks
	 *
	 * @param string $html HTML content to convert.
	 * @param string $filter DOM filter selector.
	 * @return string Serialized blocks.
	 */
	public function convert_html_to_blocks( $html, $filter = 'body' ) {
		$crawler = ( new Crawler( $html ) )->filter( $filter );
		$blocks  = [];

		if ( ! $crawler->count() ) {
			return serialize_blocks( [] );
		}

		$crawler
		->filter( $filter )
		->children()
		->each(
			function ( Crawler $node ) use ( &$blocks ) {
				$block = $this->map_node_to_block( $node );

				if ( $block ) {
					// Check if this is an array of blocks.
					if ( is_array( $block ) && ! isset( $block['blockName'] ) ) {
						$blocks = array_merge( $blocks, $block );
					} else {
						$blocks[] = $block;
					}
				} else {
					// Wrap unhandled content in a classic block
					$blocks[] = $this->create_classic_block( $node );
				}
			}
		);

		// Filter out empty blocks
		$blocks = array_filter( $blocks );

		return serialize_blocks( $blocks );
	}

	/**
	 * Maps a DOM node to a Gutenberg block
	 *
	 * @param Crawler $node DOM node to convert.
	 * @return array|null Block array or null if unmapped.
	 */
	private function map_node_to_block( Crawler $node ) {
		$tag = $node->nodeName();

		// Map specific tags to blocks
		switch ( $tag ) {
			case 'p':
				return $this->create_paragraph_block( $node );
			case 'h1':
			case 'h2':
			case 'h3':
			case 'h4':
			case 'h5':
			case 'h6':
				return $this->create_heading_block( $node, $tag );
			case 'ul':
			case 'ol':
				return $this->create_list_block( $node, 'ol' === $tag );
			case 'blockquote':
				return $this->create_quote_block( $node );
			case 'img':
				return $this->create_image_block( $node );
			case 'hr':
				return [
					'blockName'    => 'core/separator',
					'attrs'        => [],
					'innerBlocks'  => [],
					'innerHTML'    => '<hr class="wp-block-separator" />',
					'innerContent' => [ '<hr class="wp-block-separator" />' ],
				];
			default:
				return null; // Unhandled tag
		}
	}

	/**
	 * Creates a group block from a DOM node
	 *
	 * @param Crawler $node DOM node to convert.
	 * @return array Block array.
	 */
	private function create_group_block( Crawler $node ) {
		$inner_blocks = $node->children()->each(
			function ( Crawler $child_node ) {
				return $this->map_node_to_block( $child_node );
			}
		);

		return [
			'blockName'    => 'core/group',
			'attrs'        => [],
			'innerBlocks'  => array_filter( $inner_blocks ), // Remove null entries
			'innerHTML'    => $node->outerHtml(),
			'innerContent' => [],
		];
	}

	/**
	 * Creates a paragraph block from a DOM node
	 *
	 * @param Crawler $node DOM node to convert.
	 * @return array|null Block array or null if empty.
	 */
	private function create_paragraph_block( Crawler $node ) {
		if ( ! $node->text() ) {
			return null;
		}

		$inner_blocks = $node->children()->each(
			function ( Crawler $child_node ) {
				return $this->map_node_to_block( $child_node );
			}
		);

		return [
			'blockName'    => 'core/paragraph',
			'attrs'        => [],
			'innerBlocks'  => array_filter( $inner_blocks ), // Remove null entries
			'innerHTML'    => $node->outerHtml(),
			'innerContent' => [ $node->text() ],
		];
	}

	/**
	 * Creates a heading block from a DOM node
	 *
	 * @param Crawler $node DOM node to convert.
	 * @param string  $tag Heading tag (h1-h6).
	 * @return array Block array.
	 */
	private function create_heading_block( Crawler $node, $tag ) {
		$html = sprintf( '<%1$s class="wp-block-heading">%2$s</%1$s>', $tag, $node->text() );
		return [
			'blockName'    => 'core/heading',
			'attrs'        => [
				'level' => (int) substr( $tag, 1 ),
			], // Attributes can be added here if needed
			'innerBlocks'  => [],
			'innerHTML'    => $html,
			'innerContent' => [ $html ], // Full HTML as the only element in the array
		];
	}

	/**
	 * Creates a list block from a DOM node
	 *
	 * @param Crawler $node DOM node to convert.
	 * @param bool    $ordered Whether list is ordered.
	 * @return array Block array.
	 */
	private function create_list_block( Crawler $node, $ordered ) {
		return [
			'blockName'    => 'core/list',
			'attrs'        => [ 'ordered' => $ordered ],
			'innerBlocks'  => [],
			'innerHTML'    => $node->outerHtml(),
			'innerContent' => [ $node->outerHtml() ],
		];
	}

	/**
	 * Creates a quote block from a DOM node
	 *
	 * @param Crawler $node DOM node to convert.
	 * @return array Block array.
	 */
	private function create_quote_block( Crawler $node ) {
		return [
			'blockName'    => 'core/quote',
			'attrs'        => [],
			'innerBlocks'  => [],
			'innerHTML'    => $node->outerHtml(),
			'innerContent' => [ $node->text() ],
		];
	}

	/**
	 * Creates an image block from a DOM node
	 *
	 * @param Crawler $node DOM node to convert.
	 * @return array Block array.
	 */
	private function create_image_block( Crawler $node ) {
		$attrs        = [];
		$attrs['url'] = $node->attr( 'src' );
		if ( $node->attr( 'alt' ) ) {
			$attrs['alt'] = $node->attr( 'alt' );
		}
		return [
			'blockName'    => 'core/image',
			'attrs'        => $attrs,
			'innerBlocks'  => [],
			'innerHTML'    => '',
			'innerContent' => [],
		];
	}

	/**
	 * Creates a classic block from a DOM node
	 *
	 * @param Crawler $node DOM node to convert.
	 * @return array|null Block array or null if empty.
	 */
	private function create_classic_block( Crawler $node ) {
		if ( '<p></p>' === $node->outerHtml() ) {
			return null;
		}

		return [
			'blockName'    => 'core/freeform',
			'attrs'        => [],
			'innerBlocks'  => [],
			'innerHTML'    => $html,
			'innerContent' => [ $html ],
		];
	}
}
